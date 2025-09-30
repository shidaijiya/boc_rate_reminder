import smtplib
import socket
import ssl
import os
from dotenv import load_dotenv
from email.header import Header
from email.mime.text import MIMEText
from email.utils import formataddr
import socks
from lib.log_helper import log_print
from lib.utils import str_to_bool



load_dotenv()



original_socket = socket.socket
proxy_host = os.getenv("PROXY_HOST")
proxy_port = int(os.getenv("PROXY_PORT"))

smtp_server = os.getenv("SMTP_SERVER")
smtp_port = int(os.getenv("SMTP_PORT"))
sender_email = os.getenv("SENDER_EMAIL")
password = os.getenv("SENDER_PASSWD")
nickname = os.getenv('SENDER_NICKNAME')



def send_mail(receiver, subject, body, use_proxy = True):
    DEFAULT_PROXY = str_to_bool(os.getenv('DEFAULT_PROXY'))
    if DEFAULT_PROXY:
        use_proxy = True
    else:
        use_proxy = False

    msg = MIMEText(body, 'html', 'utf-8')

    msg['From'] = formataddr((str(Header(nickname, "utf-8")), sender_email))
    msg["To"] = str(Header(receiver, "utf-8"))
    msg["Subject"] = str(Header(subject, "utf-8"))

    swapped = False

    try:
        context = ssl.create_default_context()
        if use_proxy:
            socks.setdefaultproxy(socks.SOCKS5, proxy_host, proxy_port, rdns=True)
            socket.socket = socks.socksocket
            swapped = True
            log_print.info("Using proxy to send mail")
        with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context, timeout=10) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, [receiver], msg.as_string())

        log_print.info("Mail sent successfully")
        return True

    except (smtplib.SMTPException,
            smtplib.SMTPAuthenticationError,
            socket.timeout,
            TimeoutError,
            socks.ProxyConnectionError,
            socks.GeneralProxyError,
            OSError) as e:
        log_print.error(f"Mail sent failed: {e}")
        return False
    finally:
        if swapped:
            socket.socket = original_socket



