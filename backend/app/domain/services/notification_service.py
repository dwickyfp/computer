"""
Notification Service.

Handles sending notifications to external webhooks based on configuration.
"""

import httpx
import html
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.logging import get_logger
from app.domain.models.notification_log import NotificationLog
from app.domain.models.internal_notification_config import InternalNotificationConfig
from app.domain.repositories.notification_log_repo import NotificationLogRepository
from app.domain.repositories.configuration_repo import ConfigurationRepository
from app.domain.repositories.internal_notification_repo import (
    InternalNotificationConfigRepository,
)

logger = get_logger(__name__)


class NotificationService:
    """Service for managing and sending notifications."""

    def __init__(self, db: Session):
        """Initialize service."""
        self.db = db
        self.repo = NotificationLogRepository(db)
        self.config_repo = ConfigurationRepository(db)
        self.internal_repo = InternalNotificationConfigRepository(db)
        self.settings = get_settings()

    def _send_webhook(self, url: str, payload: dict) -> bool:
        """
        Send payload to webhook URL.

        Args:
            url: Webhook URL
            payload: JSON payload

        Returns:
            True if successful, False otherwise
        """
        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.post(url, json=payload)
                response.raise_for_status()
                logger.info(f"Notification sent successfully to {url}")
                return True
        except Exception as e:
            logger.error(f"Failed to send notification to {url}: {e}")
            return False

    def _send_telegram(self, bot_token: str, chat_id: str, message: str) -> bool:
        """
        Send message to Telegram using Bot API.

        Args:
            bot_token: Telegram bot token
            chat_id: Telegram chat ID or group ID
            message: Message text to send

        Returns:
            True if successful, False otherwise
        """
        try:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}

            with httpx.Client(timeout=10.0) as client:
                response = client.post(url, json=payload)
                response.raise_for_status()
                logger.info(
                    f"Notification sent successfully to Telegram chat {chat_id}"
                )
                return True
        except Exception as e:
            logger.error(f"Failed to send Telegram notification to chat {chat_id}: {e}")
            return False

    def _build_internal_html(
        self,
        title: str,
        message: str,
        notif_type: str,
        timestamp: str,
    ) -> str:
        """Build a styled HTML email body for internal notifications."""
        type_colors = {
            "ERROR": "#dc2626",
            "WARNING": "#d97706",
            "INFO": "#2563eb",
            "SUCCESS": "#16a34a",
        }
        badge_color = type_colors.get(notif_type.upper(), "#6b7280")
        safe_title = html.escape(title)
        safe_message = html.escape(message).replace("\n", "<br>")
        safe_type = html.escape(notif_type)

        return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background-color:#f3f4f6;font-family:Arial,Helvetica,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f3f4f6;padding:32px 0;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.10);">
        <tr>
          <td style="background-color:#1e293b;padding:24px 32px;">
            <p style="margin:0;color:#94a3b8;font-size:11px;letter-spacing:1.2px;text-transform:uppercase;">Rosetta ETL Platform</p>
            <h1 style="margin:8px 0 0;color:#f8fafc;font-size:20px;font-weight:600;line-height:1.3;">{safe_title}</h1>
          </td>
        </tr>
        <tr>
          <td style="padding:24px 32px 0;">
            <span style="display:inline-block;background-color:{badge_color};color:#ffffff;font-size:11px;font-weight:700;letter-spacing:0.8px;padding:4px 14px;border-radius:99px;text-transform:uppercase;">{safe_type}</span>
          </td>
        </tr>
        <tr>
          <td style="padding:16px 32px 32px;">
            <p style="margin:0;color:#374151;font-size:14px;line-height:1.8;">{safe_message}</p>
          </td>
        </tr>
        <tr><td style="padding:0 32px;"><hr style="border:none;border-top:1px solid #e5e7eb;margin:0;"></td></tr>
        <tr>
          <td style="padding:16px 32px;background-color:#f9fafb;border-radius:0 0 8px 8px;">
            <p style="margin:0;color:#6b7280;font-size:12px;">Time: {timestamp}</p>
            <p style="margin:6px 0 0;color:#9ca3af;font-size:11px;">This is an automated notification from Rosetta ETL Platform. Do not reply to this email.</p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    def _send_internal(
        self,
        cfg: InternalNotificationConfig,
        message: str,
    ) -> bool:
        """
        Send a notification to an internal HTTP email API.

        The API is called as a POST request with the message body as plain text.
        Query parameters match the reference implementation in main.py.

        Args:
            cfg: InternalNotificationConfig ORM instance
            message: Plain-text message body to send

        Returns:
            True if HTTP call succeeded (2xx), False otherwise
        """
        try:
            base = f"{cfg.base_url.rstrip('/')}/notification/emailFreeTemplate"
            params = {
                "requester": cfg.requester,
                "menuCode": cfg.menu_code,
                "companyGroupId": str(cfg.company_group_id),
                "mailFromCode": cfg.mail_from_code,
                "mailTo": cfg.mail_to,
                "subject": cfg.subject,
            }
            headers = {"Content-Type": "application/json"}
            with httpx.Client(timeout=10.0) as client:
                response = client.post(
                    base, params=params, headers=headers, content=message
                )
                response.raise_for_status()
            logger.info(
                f"Internal notification sent via config '{cfg.name}' (id={cfg.id})"
            )
            return True
        except Exception as exc:
            logger.error(
                f"Failed to send internal notification via config '{cfg.name}' (id={cfg.id}): {exc}"
            )
            return False

    def process_pending_notifications(self) -> int:
        """
        Process pending notifications and send them to webhook, Telegram, and/or internal.

        Logic:
        1. Check if webhook notifications are enabled.
        2. Check if Telegram notifications are enabled.
        3. Check if internal notifications are enabled (global flag + per-config flag).
        4. Check if webhook URL and/or Telegram credentials are configured.
        5. Fetch notifications where:
           - iteration_check >= limit (default 3)
           - is_sent = False
           - is_deleted = False
           - is_read = False
        5. Send to webhook if enabled and configured.
        6. Send to Telegram if enabled and configured.
        7. Mark as sent (regardless of webhook/Telegram availability to prevent duplicate frontend display).

        Returns:
            Number of notifications processed.
        """
        # 1. Check if webhook notifications are enabled
        enable_webhook = (
            self.config_repo.get_value(
                "ENABLE_ALERT_NOTIFICATION_WEBHOOK", "FALSE"
            ).upper()
            == "TRUE"
        )

        # 2. Check if Telegram notifications are enabled
        enable_telegram = (
            self.config_repo.get_value(
                "ENABLE_ALERT_NOTIFICATION_TELEGRAM", "FALSE"
            ).upper()
            == "TRUE"
        )

        # 3a. Check if internal notifications are enabled (global flag)
        enable_internal = (
            self.config_repo.get_value(
                "ENABLE_ALERT_NOTIFICATION_INTERNAL", "FALSE"
            ).upper()
            == "TRUE"
        )

        # 3b. Fetch enabled internal notification configs (only when global flag is on)
        internal_configs = self.internal_repo.get_enabled() if enable_internal else []

        # 3. Check configuration
        webhook_url = self.config_repo.get_value("ALERT_NOTIFICATION_WEBHOOK_URL")
        telegram_bot_token = self.config_repo.get_value(
            "ALERT_NOTIFICATION_TELEGRAM_KEY"
        )
        telegram_chat_id = self.config_repo.get_value(
            "ALERT_NOTIFICATION_TELEGRAM_GROUP_ID"
        )

        iteration_limit_str = self.config_repo.get_value(
            "NOTIFICATION_ITERATION_DEFAULT", "3"
        )
        try:
            iteration_limit = int(iteration_limit_str)
        except ValueError:
            iteration_limit = 3

        # 4. Fetch pending notifications
        # We need a custom query here as repo might not have this specific filter
        # "iteration_check is equals with config NOTIFICATION_ITERATION_DEFAULT"
        # "is_sent is false", "is_deleted is false", "is_read is false"

        pending_notifications = (
            self.db.query(NotificationLog)
            .filter(
                (NotificationLog.iteration_check >= iteration_limit)
                | (NotificationLog.is_force_sent == True),
                NotificationLog.is_sent == False,
                NotificationLog.is_deleted == False,
                NotificationLog.is_read == False,
            )
            .all()
        )

        if not pending_notifications:
            return 0

        processed_count = 0
        now = datetime.now(timezone(timedelta(hours=7)))

        # 5. Process each notification
        for notification in pending_notifications:
            # Prepare payload for webhook
            payload = {
                "key_notification": notification.key_notification,
                "title": notification.title,
                "message": notification.message,
                "type": notification.type,
                "timestamp": (
                    notification.created_at.isoformat()
                    if notification.created_at
                    else None
                ),
            }

            # Send to webhook if enabled and configured
            if enable_webhook and webhook_url:
                success = self._send_webhook(webhook_url, payload)

                if success:
                    logger.info(
                        f"Notification {notification.id} sent to webhook successfully"
                    )
            else:
                if not enable_webhook:
                    logger.debug(f"Webhook notifications are disabled")
                elif not webhook_url:
                    logger.debug(f"No webhook URL configured")

            # Send to Telegram if enabled and configured
            if enable_telegram and telegram_bot_token and telegram_chat_id:
                # Format message for Telegram with HTML
                telegram_message = (
                    f"<b>{html.escape(notification.title)}</b>\n\n"
                    f"{html.escape(notification.message)}\n\n"
                    f"Type: {html.escape(notification.type)}\n"
                    f"Time: {notification.created_at.strftime('%Y-%m-%d %H:%M:%S') if notification.created_at else 'N/A'}"
                )

                success = self._send_telegram(
                    telegram_bot_token, telegram_chat_id, telegram_message
                )

                if success:
                    logger.info(
                        f"Notification {notification.id} sent to Telegram successfully"
                    )
            else:
                if not enable_telegram:
                    logger.debug(f"Telegram notifications are disabled")
                elif not telegram_bot_token:
                    logger.debug(f"No Telegram bot token configured")
                elif not telegram_chat_id:
                    logger.debug(f"No Telegram chat ID configured")

            # Send to internal notification endpoints if enabled
            if enable_internal and internal_configs:
                ts = (
                    notification.created_at.strftime("%Y-%m-%d %H:%M:%S")
                    if notification.created_at
                    else "N/A"
                )
                internal_message = self._build_internal_html(
                    title=notification.title,
                    message=notification.message,
                    notif_type=notification.type,
                    timestamp=ts,
                )
                for cfg in internal_configs:
                    self._send_internal(cfg, internal_message)
            elif not enable_internal:
                logger.debug("Internal notifications are disabled globally")

            # 6. Mark as sent regardless of webhook/Telegram/internal availability or send status
            # This ensures notifications only appear once in frontend
            notification.is_sent = True
            notification.updated_at = now
            processed_count += 1

        if processed_count > 0:
            self.db.commit()
            logger.info(f"Processed {processed_count} notifications (marked as sent)")

        # Cleanup old notifications (older than 1 month)
        try:
            deleted_count = self.repo.delete_old_notifications(days_to_keep=30)
            if deleted_count > 0:
                logger.info(
                    f"Deleted {deleted_count} old notifications (older than 30 days)"
                )
        except Exception as e:
            logger.error(f"Failed to cleanup old notifications: {e}")

        return processed_count

    def send_test_notification(self, webhook_url: Optional[str] = None) -> bool:
        """
        Send a test notification to the configured webhook.

        Args:
            webhook_url: Optional webhook URL to use. If not provided, uses configured URL.

        Returns:
            True if successful, False otherwise
        """
        if not webhook_url:
            webhook_url = self.config_repo.get_value("ALERT_NOTIFICATION_WEBHOOK_URL")

        if not webhook_url:
            raise ValueError("Webhook URL is not configured")

        payload = {
            "key_notification": "TEST_NOTIFICATION",
            "title": "Test Notification",
            "message": "This is a test notification from Rosetta ETL Platform.",
            "type": "TEST",
            "timestamp": datetime.now(timezone(timedelta(hours=7))).isoformat(),
        }

        return self._send_webhook(webhook_url, payload)

    def send_test_telegram_notification(
        self, bot_token: Optional[str] = None, chat_id: Optional[str] = None
    ) -> bool:
        """
        Send a test notification to Telegram.

        Args:
            bot_token: Optional Telegram bot token. If not provided, uses configured token.
            chat_id: Optional Telegram chat ID. If not provided, uses configured chat ID.

        Returns:
            True if successful, False otherwise
        """
        if not bot_token:
            bot_token = self.config_repo.get_value("ALERT_NOTIFICATION_TELEGRAM_KEY")

        if not chat_id:
            chat_id = self.config_repo.get_value("ALERT_NOTIFICATION_TELEGRAM_GROUP_ID")

        if not bot_token:
            raise ValueError("Telegram bot token is not configured")

        if not chat_id:
            raise ValueError("Telegram chat ID is not configured")

        # Format test message for Telegram with HTML
        message = (
            "<b>Test Notification</b>\n\n"
            "This is a test notification from Rosetta ETL Platform.\n\n"
            "Type: TEST\n"
            f"Time: {datetime.now(timezone(timedelta(hours=7))).strftime('%Y-%m-%d %H:%M:%S')}"
        )

        return self._send_telegram(bot_token, chat_id, message)

    def send_test_internal_notification(self, cfg: InternalNotificationConfig) -> bool:
        """
        Send a test notification using a specific internal notification config.

        Args:
            cfg: InternalNotificationConfig ORM instance to test

        Returns:
            True if the HTTP call succeeded, False otherwise
        """
        now_str = datetime.now(timezone(timedelta(hours=7))).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        message = self._build_internal_html(
            title="Test Notification",
            message="This is a test notification from Rosetta ETL Platform.",
            notif_type="INFO",
            timestamp=now_str,
        )
        return self._send_internal(cfg, message)
