import csv
import datetime
import time
import os
import emails
import logging
from dotenv import load_dotenv
from pyairtable.api import Api as AirtableApi
from rate_limiter.python.package_throttler import PackageThrottler


airtable_throttle = PackageThrottler((), max_operations_in_window=5, rate_limit_window=1).execute_with_throttle

load_dotenv()

CSV_FILENAME = "coaching_report.csv"

def send_email(to_email: str, items, map, attachment=False) -> None:
    def mapName(id):
        return map.get(id, '')['name'] if map.get(id, '') else "*Employee Not Found*"
    
    from_name = os.getenv("EMAIL_FROM_NAME")
    from_addr = os.getenv("EMAIL_FROM")
    table_rows = ""
    for i in items['managers']:
        table_rows += f"""
            <tr>
                <td style="width:auto; white-space:nowrap; border:1px solid #ddd; padding:8px;">{mapName(i['manager_id'])}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: center;">{i['num_of_employees']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: center;">{i['coaching_logs']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: center;">{i['compliance_percentage']:.0f}%</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{i['notes']}</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: center;">{i['status']}</td>
            </tr>
        """


    # cc_emails = [
    #     "yves.gabiana@myamazonguy.com",
    #     "eunice.cruz@myamazonguy.com"
    # ]
    html_body = f"""
    <html>
    <body>
        <p>Hello {mapName(items['director_id'])}!</p>
        <p>Please see below the Weekly Coaching Compliance Report for your Husk.</p>
        <h2>Coaching Compliance Report</h2>
        <table style="border-collapse: collapse; width: 100%;">
            <tr style="background-color: #4CAF50; color: white;">
                <th style="border: 1px solid #ddd; padding: 8px;">Manager</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Direct Reports</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Coaching Logs</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Compliance %</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Notes</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Status</th>
            </tr>
            {table_rows}
        </table>
        <p>This automated report is sent every Friday EOD to help you track coaching activity and ensure all \n manager remain compliant.</p>
        <p>if you have any questions or need adjustments to the report, please let us know.</p>
        <p>Thank you!</p>
    </body>
    </html>
    """

    attempts = 0
    max_attempts = 2
    while attempts < max_attempts:
        try:
            message = emails.html(
                subject="Weekly Coaching Compliance Report",
                html=html_body,
                text="will add this later",
                mail_from=(from_name, from_addr),
                #cc=cc_emails
            )
            today = datetime.date.today()
            week_num = today.isocalendar()[1]
            fileName = CSV_FILENAME.replace(".csv", f'_week{week_num}.csv')
            if attachment:
                with open(CSV_FILENAME, "rb") as f: 
                    message.attach( filename=fileName, data=f.read(), mime_type="text/csv" ) #W10_coaching_compliance_report.csv

            response = message.send(
                to=to_email,
                smtp={
                    "host": "smtp.gmail.com",
                    "port": 465,
                    "user": os.getenv("GMAIL_ADDRESS"),
                    "password": os.getenv("GMAIL_APP_PASSWORD"),
                    "ssl": True
                }
            )
            break
        except Exception as e:
            attempts += 1
            logging.error(f"Exception occurred while sending email to {to_email}: {e} \n\tAttempt {attempts} of {max_attempts}")
            time.sleep(2)
            return
    
    if response.status_code == 250:
        print(f"Email sent to {to_email} successfully.")
    else:
        logging.error(f"Failed to send email to {to_email}. "
                      f"SMTP status={response.status_code}")
        
def get_table(tab, view):
    x = 0
    while x <= 2:
        try:
            airtable = AirtableApi(os.getenv("AIRTABLE_API_KEY"))
            table = airtable.table("appfccXiah8EtMfbZ", tab)
            records = airtable_throttle(table, 'all', view=view)
            return records
        except Exception as e:
            x += 1
            logging.error(f"Attempt {x} failed: {e}")

    logging.error("Failed to retrieve Airtable records after 2 attempts")
    return None

def getDirectory(workers):
    directory = []
    for record in workers:
        fields = record.get('fields', {})
        director_id = fields.get('Brand Director')
        manager_id = fields.get('Manager') if fields.get('Manager') else director_id
        employee_id = record.get('id')

        if director_id and fields.get('Worker') != 'Steven Pope':
            directory.append({
                "employee_id": employee_id,
                "manager_id": manager_id[0],
                "director_id": director_id[0]
            })
    return directory

def getCoacingDirectory(coaching_calls):
    directory = []
    for record in coaching_calls:
        fields = record.get('fields', {})
        coach_id = fields.get('Coach')
        trainee_id = fields.get('Trainee')
        directory.append({
            "coach_id": coach_id[0] if coach_id else None,
            "trainee_id": trainee_id[0] if trainee_id else None,
            # "created_at": fields.get('Created At'),
            # "trainee_approval": fields.get('Trainee Approval'),
            # "coaching_type": fields.get('Coaching Type'),
            # "feedback_type": fields.get('Feedback: Type'),
            # "feedback_issue": fields.get('Feedback: Issue'),
            # "feedback_solution/expectations": fields.get('Feedback: Solution/Expectations'),
            # "incident": fields.get('Incident'),
            # "incident_performance_expectations": fields.get('Incident: Performance Expectations'),
            # "incident_solution/path_forward": fields.get('Incident: Solution/Path Forward'),
            # "coaching_what_they_do_well": fields.get('Coaching: What they do well'),
            # "coaching_what_they_can_improve_on": fields.get('Coaching: What they can improve on'),
            # "coaching_short_term_goals": fields.get('Coaching: Short-term goals')
        })
    return directory
    
def main(dev_mode=False):
    print(f"Running in {'development' if dev_mode else 'production'} mode")

    workers = get_table("Workers", "Active Workers")
    name_map = {worker['id']: {'name': worker['fields'].get('Worker'), 'email': worker['fields'].get('Work Email Address copy')} for worker in workers}
    directory = getDirectory(workers)

    coaching_calls = get_table('tblA4AbLZQcdgi0RC', 'viwdYEfy7lkI2XCsr')
    coaching_directory = getCoacingDirectory(coaching_calls)

    data = []
    directors = set([director['director_id'] for director in directory])
    for director in directors:
        list_sub_items = []
        managers = set([item['manager_id'] for item in directory if item['director_id'] == director])
        attachment = []
        for manager in managers:
            if manager != director:
                employees = set([item['employee_id'] for item in directory if item['manager_id'] == manager and item['director_id'] == director])
                tranings_done = 0
                for employee in employees:
                    coaching_logs = [log for log in coaching_directory if log['coach_id'] == manager and log['trainee_id'] == employee]

                    logs = [{
                        "Manager": name_map[manager]['name'] if name_map.get(manager) else "Unknown Manager",
                        "Employee": name_map[employee]['name'] if name_map.get(employee) else "Unknown Employee",
                        "Coached": "Yes" if coaching_logs else "No"
                    }]
                    attachment.extend(logs)

                    if coaching_logs:
                        tranings_done += 1

                percentage = (tranings_done/len(employees))*100 if len(employees) > 0 else 0
                notes =  f"Follow-up required"if percentage < 85 else f"{len(employees) - tranings_done} Pending"
                status = "❌" if percentage < 85 else "⚠️"
                sub_item = {
                    "manager_id": manager, 
                    "num_of_employees": len(employees), 
                    "coaching_logs": tranings_done, 
                    "compliance_percentage": percentage,
                    "notes": "Complete" if percentage == 100 else notes,
                    "status": "✅" if percentage == 100 else status
                }
                list_sub_items.append(sub_item)
            else: continue
        if list_sub_items:
            item = {"director_id": director, "managers": list_sub_items, "attachment": attachment}
            data.append(item)
    
    for item in data:
        director_email = name_map[item['director_id']]['email'] if not dev_mode else os.getenv("TEST_EMAIL")

        #log for file attachment
        if item['attachment']:
            with open(CSV_FILENAME, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=item['attachment'][0].keys()) 
                writer.writeheader() 
                writer.writerows(item['attachment'])

        send_email(to_email=director_email, items=item, map=name_map, attachment= bool(item['attachment']))


if __name__ == "__main__":
    dev_mode = os.getenv("DEV_MODE", "False").lower() == "true"

    print(" --------------- Starting execution")
    main(dev_mode=dev_mode)
    print(" --------------- Finished execution")