"""Microsoft Graph email helpers."""

from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


GRAPH_SCOPE = "https://graph.microsoft.com/.default"
GRAPH_SENDMAIL_URL = "https://graph.microsoft.com/v1.0/users/{sender}/sendMail"


@dataclass(frozen=True)
class GraphSettings:
    tenant_id: str
    client_id: str
    client_secret: str
    sender: str


def load_graph_settings() -> GraphSettings:
    tenant_id = os.getenv("GRAPH_TENANT_ID", "").strip()
    client_id = os.getenv("GRAPH_CLIENT_ID", "").strip()
    client_secret = os.getenv("GRAPH_CLIENT_SECRET", "").strip()
    sender = os.getenv("GRAPH_SENDER_EMAIL", "").strip() or os.getenv("OUTLOOK_EMAIL", "").strip()

    missing = [
        name
        for name, value in {
            "GRAPH_TENANT_ID": tenant_id,
            "GRAPH_CLIENT_ID": client_id,
            "GRAPH_CLIENT_SECRET": client_secret,
            "GRAPH_SENDER_EMAIL": sender,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Configure {', '.join(missing)} no .env para enviar e-mails via Microsoft Graph.")

    return GraphSettings(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        sender=sender,
    )


def _read_response_body(error: urllib.error.HTTPError) -> str:
    try:
        return error.read().decode("utf-8", errors="replace")
    except Exception:
        return str(error)


def _post_form(url: str, data: dict[str, str]) -> dict[str, Any]:
    encoded = urllib.parse.urlencode(data).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=encoded,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Falha ao autenticar no Microsoft Graph: {_read_response_body(exc)}") from exc


def _post_json(url: str, headers: dict[str, str], payload: dict[str, Any]) -> None:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={**headers, "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            if response.status not in (200, 202):
                raise RuntimeError(f"Microsoft Graph retornou HTTP {response.status}.")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Falha ao enviar e-mail pelo Microsoft Graph: {_read_response_body(exc)}") from exc


def _acquire_graph_token(settings: GraphSettings) -> str:
    token_url = f"https://login.microsoftonline.com/{settings.tenant_id}/oauth2/v2.0/token"
    token_response = _post_form(
        token_url,
        {
            "client_id": settings.client_id,
            "client_secret": settings.client_secret,
            "scope": GRAPH_SCOPE,
            "grant_type": "client_credentials",
        },
    )
    access_token = str(token_response.get("access_token") or "").strip()
    if not access_token:
        raise RuntimeError(f"Microsoft Graph não retornou access_token: {token_response}")
    return access_token


def _recipient(address: str) -> dict[str, dict[str, str]]:
    return {"emailAddress": {"address": address}}


def send_email_with_attachments(
    settings: GraphSettings,
    *,
    to: str,
    cc: list[str] | None,
    subject: str,
    body: str,
    attachments: list[tuple[str, bytes]],
) -> None:
    token = _acquire_graph_token(settings)
    clean_cc = [email.strip() for email in (cc or []) if str(email).strip()]
    payload = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body,
            },
            "toRecipients": [_recipient(to)],
            "ccRecipients": [_recipient(email) for email in clean_cc],
            "attachments": [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": filename,
                    "contentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "contentBytes": base64.b64encode(content).decode("ascii"),
                }
                for filename, content in attachments
            ],
        },
        "saveToSentItems": True,
    }
    sender = urllib.parse.quote(settings.sender, safe="")
    _post_json(
        GRAPH_SENDMAIL_URL.format(sender=sender),
        {"Authorization": f"Bearer {token}"},
        payload,
    )
