import json
import unittest
from unittest.mock import Mock, patch

from commission_tool.io.email import GraphSettings, send_email_with_attachments


class GraphEmailTests(unittest.TestCase):
    @patch("commission_tool.io.email._post_json")
    @patch("commission_tool.io.email._acquire_graph_token", return_value="token-123")
    def test_send_email_uses_graph_sendmail_with_recipients_cc_and_attachment(self, token_mock, post_mock):
        settings = GraphSettings(
            tenant_id="tenant",
            client_id="client",
            client_secret="secret",
            sender="sender@example.com",
        )

        send_email_with_attachments(
            settings,
            to="to@example.com",
            cc=["copy@example.com"],
            subject="Relatorio",
            body="Segue anexo.",
            attachments=[("relatorio.xlsx", b"abc")],
        )

        token_mock.assert_called_once_with(settings)
        post_mock.assert_called_once()
        url = post_mock.call_args.args[0]
        headers = post_mock.call_args.args[1]
        payload = post_mock.call_args.args[2]

        self.assertEqual(url, "https://graph.microsoft.com/v1.0/users/sender%40example.com/sendMail")
        self.assertEqual(headers["Authorization"], "Bearer token-123")
        self.assertEqual(payload["message"]["toRecipients"][0]["emailAddress"]["address"], "to@example.com")
        self.assertEqual(payload["message"]["ccRecipients"][0]["emailAddress"]["address"], "copy@example.com")
        self.assertEqual(payload["message"]["attachments"][0]["@odata.type"], "#microsoft.graph.fileAttachment")
        self.assertEqual(payload["message"]["attachments"][0]["contentBytes"], "YWJj")
        self.assertTrue(payload["saveToSentItems"])


if __name__ == "__main__":
    unittest.main()
