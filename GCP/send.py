from flask import Flask, request, jsonify
import apprise

app = Flask(__name__)

def build_message(status, job_name, start_time, end_time, additional_info=None):
    """
    Builds a success or failure message template based on the input status.
    """
    if status == "success":
        message = {
            "status": "success",
            "job_name": job_name,
            "start_time": start_time,
            "end_time": end_time,
            "message": f"The job '{job_name}' completed successfully.",
            "details": additional_info or "No additional details."
        }
    elif status == "failure":
        message = {
            "status": "failure",
            "job_name": job_name,
            "start_time": start_time,
            "end_time": end_time,
            "message": f"The job '{job_name}' failed.",
            "details": additional_info or "No additional details provided."
        }
    else:
        return {"error": "Invalid status. Use 'success' or 'failure'."}

    return message


def send_email_notification(subject, body):
    """
    Sends email notification using Apprise with error handling.
    """
    import apprise

    apobj = apprise.Apprise()

    # Add your Hotmail email service URL and recipient
   
    email_url ='mailto://karthikeyan046:@gmail.com?to=bhavani.victory12@gmail.com'
    apobj.add(email_url) 
    try:
        # Attempt to send the email
        success = apobj.notify(
            body=body,
            title=subject
        )
        if success:
            print("Email sent successfully!")
        else:
            print("Failed to send email. Please check your credentials and configuration.")
    except Exception as e:
        print(f"An error occurred while sending email: {e}")

@app.route('/notify', methods=['POST'])
def notify():
    data = request.json
    
    # Extract parameters from the incoming request
    status = data.get("status")
    job_name = data.get("job_name")
    start_time = data.get("start_time")
    end_time = data.get("end_time")
    additional_info = data.get("additional_info", None)

    # Build the message dynamically
    response = build_message(status, job_name, start_time, end_time, additional_info)

    # Extract subject and body for email
    subject = f"ETL Job {response['status'].capitalize()}: {job_name}"
    body = (
        f"Job Name: {response['job_name']}\n"
        f"Status: {response['status']}\n"
        f"Start Time: {response['start_time']}\n"
        f"End Time: {response['end_time']}\n"
        f"Message: {response['message']}\n"
        f"Details: {response['details']}"
    )

    # Send email notification
    send_email_notification(subject, body)

    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)
