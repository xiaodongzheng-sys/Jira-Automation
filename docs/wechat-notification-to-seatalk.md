# WeChat Notification To SeaTalk Bridge

This local Mac watcher forwards selected WeChat desktop notifications to a SeaTalk webhook.

It does not use WeChat private protocols, OCR, or UI scraping. WeChat system notifications must be enabled for `WeChat.app`.

## Configuration

Add the webhook to `.env`:

```bash
SEATALK_WEBHOOK_URL=https://openapi.seatalk.io/webhook/group/...
```

Optional settings:

```bash
WECHAT_ALERT_INCLUDE_PREVIEW=true
WECHAT_ALERT_DEDUPE_MINUTES=10
WECHAT_ALERT_POLL_SECONDS=2
WECHAT_ALERT_LOOKBACK_MINUTES=30
WECHAT_ALERT_NOTIFICATION_DB=/absolute/path/to/notification/db
WECHAT_REPLY_TTL_HOURS=24
WECHAT_REPLY_SERVER_HOST=127.0.0.1
WECHAT_REPLY_SERVER_PORT=8797
WECHAT_REPLY_CALLBACK_TOKEN=change-me
WECHAT_REPLY_ALLOWED_SENDERS=your.email@example.com
WECHAT_REPLY_RESTORE_FRONT_APP=true
WECHAT_REPLY_HIDE_WECHAT_AFTER_SEND=true
```

## Test foreground mode

```bash
scripts/run_wechat_notification_to_seatalk_foreground.sh --diagnose
scripts/run_wechat_notification_to_seatalk_foreground.sh --dry-run --once
scripts/run_wechat_notification_to_seatalk_foreground.sh
```

Use `--dry-run` first. Send yourself a WeChat private message and a group `@` mention, then confirm the alert text looks right before sending to SeaTalk.

## Install launchd mode

```bash
scripts/install_wechat_notification_to_seatalk_launchd.sh
launchctl start io.npt.wechat-notification-to-seatalk
```

Logs are written under `TEAM_PORTAL_DATA_DIR/logs`, defaulting to `.team-portal/logs`.

## Notes

- Private messages are forwarded when the notification looks like a one-to-one chat.
- Group messages are forwarded only when the notification text indicates an `@` mention.
- Normal group messages without an `@` mention are ignored.
- If notification previews are hidden, macOS may not expose enough text to distinguish all cases.

## Reply from SeaTalk back to WeChat

Forwarded alerts include the WeChat conversation, message preview, timestamp, and a copy-ready reply command:

```text
WX DM | Alice

Can you help check this?

Time: 2026-04-28 22:18:31 +08

Reply: /reply wx_1234567890 <message>
```

Start the local callback server:

```bash
scripts/run_seatalk_reply_to_wechat_foreground.sh --dry-run
```

Configure the SeaTalk Open Platform Bot callback URL to point to:

```text
http://127.0.0.1:8797/seatalk/wechat-reply?token=change-me
```

For a real SeaTalk callback from outside the Mac, expose this local server through the existing tunnel pattern or another trusted HTTPS tunnel, then use the public HTTPS URL in SeaTalk Open Platform.

When a SeaTalk user sends:

```text
/reply wx_1234567890 收到，我晚点看
```

the server looks up `wx_1234567890` in the local reply map and sends the message to the original WeChat conversation through WeChat Desktop.

Install launchd mode:

```bash
scripts/install_seatalk_reply_to_wechat_launchd.sh
launchctl start io.npt.seatalk-reply-to-wechat
```

WeChat Desktop must be logged in. macOS may ask for Accessibility permission because the sender uses UI automation to activate WeChat, search the conversation, paste the reply, and press Enter.
