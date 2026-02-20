import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# === Gmail Credentials ===
SENDER_EMAIL = "sridhargoudu7@gmail.com"
APP_PASSWORD = "eajavyuglmegekwh"
RECEIVER_EMAIL = "s190204@rguktsklm.ac.in"

# === Email Content ===
subject = "Test Email from Python"
body = "Hi, this is a test email with Excel attachment sent using Python."

# === Create Message ===
msg = MIMEMultipart()
msg["From"] = SENDER_EMAIL
msg["To"] = RECEIVER_EMAIL
msg["Subject"] = subject
msg.attach(MIMEText(body, "plain"))

# === Attach Excel File ===
filename = "TEATER_DAILY_USAGE.xlsx"  # Excel file generated in step 2
with open(filename, "rb") as attachment:
    part = MIMEBase("application", "octet-stream")
    part.set_payload(attachment.read())

encoders.encode_base64(part)
part.add_header("Content-Disposition", f"attachment; filename={filename}")
msg.attach(part)

# === Send Email ===
try:
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(SENDER_EMAIL, APP_PASSWORD)
    server.send_message(msg)
    print("✅ Email sent successfully!")
except Exception as e:
    print(f"❌ Error: {e}")
finally:
    server.quit()

### for actiivating the repo ...
