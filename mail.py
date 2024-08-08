import os
import json
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def send_email(event, context):
    # Get the message payload from Pub/Sub
    message = event['data']
    
    # Extract the email content from the message
    email_content = message['email_content']
    
    # Set up SendGrid credentials
    sendgrid_api_key = os.environ['SENDGRID_API_KEY']
    
    # Create a SendGrid client
    sendgrid_client = SendGridAPIClient(sendgrid_api_key)
    
    # Create a Mail object
    message = Mail(
        from_email='your-email@example.com',
        to_emails='recipient-email@example.com',
        subject='Test email',
        plain_text_content=email_content
    )
    
    # Send the email using SendGrid
    response = sendgrid_client.send(message)
    
    # Check if the email was sent successfully
    if response.status_code == 202:
        print('Email sent successfully!')
    else:
        print('Error sending email:', response.body)