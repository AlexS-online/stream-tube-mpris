//! Linux: publishes the playing video over MPRIS (org.mpris.MediaPlayer2.streamtube), so desktop
//! shells and phone remotes (KDE Connect / GSConnect) show a media card with transport buttons.
//! The Windows counterpart (SMTC) lives in media_controls.rs.
use crate::player::controller::{Player, PlayerHandle, player};
use futures::{StreamExt, channel::mpsc};
use gpui_kit::{App, Entity, Subscription};
use mpris_server::{Metadata, PlaybackStatus, Player as Mpris, Time, TrackId};

#[derive(Clone, Copy, PartialEq)]
enum Status {
    Closed,
    Stopped,
    Paused,
    Playing,
}

#[derive(Clone, PartialEq)]
struct State {
    /// id, title, channel, thumbnail
    video: Option<(String, String, String, String)>,
    status: Status,
    next: bool,
    previous: bool,
    duration: f64,
    time: f64,
}

impl State {
    fn of(player: &Player) -> State {
        let video = player
            .video
            .as_ref()
            .map(|v| (v.id.clone(), v.title.clone(), v.channel_name.clone(), v.thumbnail.clone()));
        let status = if video.is_none() || player.error.is_some() {
            Status::Closed
        } else if player.eof {
            Status::Stopped
        } else if player.paused {
            Status::Paused
        } else {
            Status::Playing
        };
        State {
            video,
            status,
            next: player.queue_pos.is_some_and(|i| i + 1 < player.queue.len()),
            previous: player.queue_pos.is_some_and(|i| i > 0),
            duration: player.duration,
            time: player.time,
        }
    }
}

enum Cmd {
    Play,
    Pause,
    PlayPause,
    Stop,
    Next,
    Previous,
    SeekBy(f64),
    SeekTo(f64),
}

fn track_id(id: &str) -> TrackId {
    let clean: String = id.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '_').collect();
    TrackId::try_from(format!("/org/streamtube/track/{clean}")).unwrap_or(TrackId::NO_TRACK)
}

async fn publish(mpris: &Mpris, last: &mut Option<State>, state: State) {
    let changed_video = last.as_ref().map(|s| &s.video) != Some(&state.video);
    if changed_video {
        let metadata = match &state.video {
            Some((id, title, channel, thumbnail)) => {
                let mut builder = Metadata::builder().trackid(track_id(id)).title(title.clone()).artist([channel.clone()]);
                if state.duration > 0.0 {
                    builder = builder.length(Time::from_micros((state.duration * 1_000_000.0) as i64));
                }
                if thumbnail.starts_with("http") {
                    builder = builder.art_url(thumbnail.clone());
                }
                builder.build()
            }
            None => Metadata::new(),
        };
        let _ = mpris.set_metadata(metadata).await;
    }
    mpris.set_position(Time::from_micros((state.time.max(0.0) * 1_000_000.0) as i64));
    let status = match state.status {
        Status::Closed | Status::Stopped => PlaybackStatus::Stopped,
        Status::Paused => PlaybackStatus::Paused,
        Status::Playing => PlaybackStatus::Playing,
    };
    let open = state.status != Status::Closed;
    if last.as_ref().map(|s| s.status) != Some(state.status) {
        let _ = mpris.set_playback_status(status).await;
        let _ = mpris.set_can_play(open).await;
        let _ = mpris.set_can_pause(open).await;
        let _ = mpris.set_can_seek(open).await;
    }
    if last.as_ref().map(|s| s.next) != Some(state.next) {
        let _ = mpris.set_can_go_next(state.next).await;
    }
    if last.as_ref().map(|s| s.previous) != Some(state.previous) {
        let _ = mpris.set_can_go_previous(state.previous).await;
    }
    *last = Some(state);
}

fn spawn_server(mut states: mpsc::UnboundedReceiver<State>, cmds: mpsc::UnboundedSender<Cmd>) {
    let _ = std::thread::Builder::new().name("mpris".into()).spawn(move || {
        futures::executor::block_on(async move {
            let Ok(mpris) = Mpris::builder("streamtube")
                .identity("Stream Tube")
                .desktop_entry("streamtube")
                .can_play(false)
                .can_pause(false)
                .can_seek(false)
                .can_go_next(false)
                .can_go_previous(false)
                .can_control(true)
                .build()
                .await
            else {
                return;
            };
            let tx = cmds.clone();
            mpris.connect_play(move |_| { let _ = tx.unbounded_send(Cmd::Play); });
            let tx = cmds.clone();
            mpris.connect_pause(move |_| { let _ = tx.unbounded_send(Cmd::Pause); });
            let tx = cmds.clone();
            mpris.connect_play_pause(move |_| { let _ = tx.unbounded_send(Cmd::PlayPause); });
            let tx = cmds.clone();
            mpris.connect_stop(move |_| { let _ = tx.unbounded_send(Cmd::Stop); });
            let tx = cmds.clone();
            mpris.connect_next(move |_| { let _ = tx.unbounded_send(Cmd::Next); });
            let tx = cmds.clone();
            mpris.connect_previous(move |_| { let _ = tx.unbounded_send(Cmd::Previous); });
            let tx = cmds.clone();
            mpris.connect_seek(move |_, offset| {
                let _ = tx.unbounded_send(Cmd::SeekBy(offset.as_micros() as f64 / 1_000_000.0));
            });
            let tx = cmds.clone();
            mpris.connect_set_position(move |_, _, position| {
                let _ = tx.unbounded_send(Cmd::SeekTo(position.as_micros() as f64 / 1_000_000.0));
            });
            let updates = async {
                let mut last: Option<State> = None;
                while let Some(state) = states.next().await {
                    if last.as_ref() != Some(&state) {
                        publish(&mpris, &mut last, state).await;
                    }
                }
            };
            futures::future::join(mpris.run(), updates).await;
        });
    });
}

fn observe_player(active: &Entity<Player>, states: mpsc::UnboundedSender<State>, cx: &mut App) -> Subscription {
    let _ = states.unbounded_send(State::of(active.read(cx)));
    cx.observe(active, move |entity, cx| {
        let _ = states.unbounded_send(State::of(entity.read(cx)));
    })
}

pub(crate) fn init(cx: &mut App) {
    let (state_tx, state_rx) = mpsc::unbounded();
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded();
    spawn_server(state_rx, cmd_tx);
    let active = player(cx);
    let mut binding = (active.entity_id(), observe_player(&active, state_tx.clone(), cx));
    let states = state_tx.clone();
    cx.observe_global::<PlayerHandle>(move |cx| {
        let active = player(cx);
        if binding.0 != active.entity_id() {
            binding = (active.entity_id(), observe_player(&active, states.clone(), cx));
        }
    })
    .detach();
    cx.spawn(async move |cx| {
        while let Some(cmd) = cmd_rx.next().await {
            let _ = cx.update(|cx| {
                player(cx).update(cx, |p, cx| {
                    if p.video.is_none() {
                        return;
                    }
                    match cmd {
                        Cmd::Play if p.eof || p.paused => {
                            if p.eof {
                                p.toggle_pause(cx)
                            } else {
                                p.set_paused(false, cx)
                            }
                        }
                        Cmd::Play => {}
                        Cmd::Pause => p.set_paused(true, cx),
                        Cmd::PlayPause => p.toggle_pause(cx),
                        Cmd::Stop => p.stop(cx),
                        Cmd::Next => {
                            if let Some(index) = p.queue_pos.filter(|i| i + 1 < p.queue.len()) {
                                p.play_queue_index(index + 1, cx);
                            }
                        }
                        Cmd::Previous => {
                            if let Some(index) = p.queue_pos.filter(|i| *i > 0) {
                                p.play_queue_index(index - 1, cx);
                            }
                        }
                        Cmd::SeekBy(seconds) => p.seek_by(seconds),
                        Cmd::SeekTo(seconds) => p.seek_to(seconds),
                    }
                    cx.notify();
                })
            });
        }
    })
    .detach();
}
