from config import ConfigClass
import requests
import json

class SrvEmail():
    def send(self, subject, receiver, sender, content="", msg_type="plain", template=None, template_kwargs={}):
        url = ConfigClass.EMAIL_SERVICE
        payload = {
            "subject": subject,
            "sender": sender,
            "receiver": [receiver],
            "msg_type": msg_type,
        }
        if content:
            payload["message"] = content
        if template:
            payload["template"] = template
            payload["template_kwargs"] = template_kwargs
        res = requests.post(
            url=url,
            json=payload
        )
        return json.loads(res.text)

