import base64
import functions_framework
import base64
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    pubsub_message=cloud_event.data["message"]["data"]
    email, message = pubsub_message.split(':', 1)
   
    gmail_user = os.environ.get('GMAIL_USER')
    gmail_password = os.environ.get('GMAIL_PASSWORD')  # Use App Password if 2-Step Verification is enabled
    gmail_user='karthi20241@hotmail.com'
        print(gmail_user)
    print(gmail_password)

    # Create the email content
    msg = MIMEMultipart()
    msg['From'] = gmail_user
    msg['To'] = email
    msg['Subject'] = 'Alert Notification Working in GCP'
    msg['Cc'] = 'bhavani.victory12@gmail.com'
    msg.attach(MIMEText(message, 'plain'))
   
    try:
        # Connect to the Gmail SMTP server and send the email
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server = smtplib.SMTP('smtp.office365.com', 587)
        server.starttls()
        server.login(gmail_user, gmail_password)
        server.sendmail(gmail_user, email, msg.as_string())
        server.quit()
        print(f"Email sent to {email}")
    except Exception as e:
        print(f"Error sending email: {e}")
  

    print("hi")
