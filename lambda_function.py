import pandas as pd
from io import BytesIO
import boto3
from sqlalchemy import create_engine
import os
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# ==========================================
# DATABASE CONNECTION
# ==========================================
DB_USER = "prod-read-user"
DB_PASS = 'UY8C&"W>&A6I*g$WTCbb50rn'
DB_HOST = "proddb-read-replica.crhg7zleeuhf.ap-south-1.rds.amazonaws.com"
DB_PORT = "3306"
DB_NAME = "edwisely_college"

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# ==========================================
# HELPER FUNCTION: Execute Query
# ==========================================
def execute_query(query):
    """Execute SQL query and return DataFrame"""
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        print(f"Query execution error: {e}")
        raise

# ==========================================
# DATA EXTRACTION QUERIES
# ==========================================

def get_base_colleges():
    query = """
    SELECT DISTINCT    
        c.id as college_id,
        c.college_name,
        ay.name 
    FROM college_academic_years cay 
    JOIN academic_years ay ON ay.id = cay.academic_year_id 
    JOIN regulation_batch_mapping rbm ON rbm.id = cay.regulation_batch_mapping_id 
    JOIN regulation_mappings rm ON rm.id = rbm.regulation_mapping_id
    JOIN college_university_degree_department_new cuddn ON cuddn.id = rm.cudd_id 
    JOIN college c ON cuddn.college_id = c.id
    WHERE ay.current_academic_year = 1
    """
    return execute_query(query)

def get_teach_data():
    """Get TEACH module data"""
    base_df = get_base_colleges()
    
    # Live Assignments
    live_assignment_df = execute_query("""
        SELECT 
            c.id AS college_id,
            c.college_name,
            COUNT(DISTINCT fch.id) AS attendance_count
        FROM college c
        LEFT JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
        LEFT JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
        LEFT JOIN faculty_class_hours fch ON fch.faculty_id = can.id
            AND fch.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
            AND CONCAT(CURDATE(), ' 08:00:00')
        GROUP BY c.id, c.college_name
    """)
    
    # Hook Count
    hook_count_df = execute_query("""
        SELECT 
            c.id AS college_id,
            c.college_name,
            COUNT(DISTINCT cphs.id) AS hook_search_count
        FROM college c
        LEFT JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
        LEFT JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
        LEFT JOIN college_account_co_pilot_search cacps ON cacps.college_account_id = can.id
        LEFT JOIN co_pilot_hook_search cphs ON cphs.college_account_co_pilot_search_id = cacps.id
            AND cphs.date_of_search BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
            AND CONCAT(CURDATE(), ' 08:00:00')
        GROUP BY c.id, c.college_name
    """)
    
    # Teach Studio
    teach_studio_df = execute_query("""
        SELECT 
            c.id AS college_id,
            c.college_name,
            COUNT(DISTINCT cphse.id) AS teach_studio_count
        FROM college c
        LEFT JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
        LEFT JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
        LEFT JOIN college_account_co_pilot_search cacps ON cacps.college_account_id = can.id
        LEFT JOIN co_pilot_hook_search cphs ON cphs.college_account_co_pilot_search_id = cacps.id
        LEFT JOIN co_pilot_hook_search_elaborate cphse ON cphse.co_pilot_hook_search_id = cphs.id
            AND cphse.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
            AND CONCAT(CURDATE(), ' 08:00:00')
        GROUP BY c.id, c.college_name
    """)
    
    # Merge all
    final_df = (base_df
        .merge(live_assignment_df, on=["college_id", "college_name"], how="left")
        .merge(hook_count_df, on=["college_id", "college_name"], how="left")
        .merge(teach_studio_df, on=["college_id", "college_name"], how="left"))
    
    return final_df.fillna(0)

def get_engage_data():
    """Get ENGAGE module data"""
    base_df = get_base_colleges()
    
    queries = {
        "questionnaire_live_count": """
            SELECT c.id AS college_id, c.college_name, COUNT(DISTINCT ql.id) AS questionnaire_live_count
            FROM college c
            LEFT JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
            LEFT JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
            LEFT JOIN questionnaire_live ql ON ql.college_account_id = can.id
                AND ql.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                AND CONCAT(CURDATE(), ' 08:00:00')
            GROUP BY c.id, c.college_name
        """,
        "live_survey_count": """
            SELECT c.id AS college_id, c.college_name, COUNT(DISTINCT ls.id) AS live_survey_count
            FROM college c
            LEFT JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
            LEFT JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
            LEFT JOIN live_surveys ls ON ls.college_account_id = can.id
                AND ls.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                AND CONCAT(CURDATE(), ' 08:00:00')
            GROUP BY c.id, c.college_name
        """,
        "notifications_count": """
            SELECT c.id AS college_id, c.college_name, COUNT(DISTINCT n.id) AS notifications_count
            FROM college c
            LEFT JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
            LEFT JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
            LEFT JOIN notifications n ON n.college_account_id = can.id
                AND n.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                AND CONCAT(CURDATE(), ' 08:00:00')
            GROUP BY c.id, c.college_name
        """,
        "video_conference_live_classes_count": """
            SELECT c.id AS college_id, c.college_name, COUNT(DISTINCT vc.id) AS video_conference_live_classes_count
            FROM college c
            LEFT JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
            LEFT JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
            LEFT JOIN video_conference vc ON vc.college_account_id = can.id
                AND vc.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                AND CONCAT(CURDATE(), ' 08:00:00')
            GROUP BY c.id, c.college_name
        """,
        "academic_projects_count": """
            SELECT c.id AS college_id, c.college_name, COUNT(DISTINCT ap.id) AS academic_projects_count
            FROM college c
            LEFT JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
            LEFT JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
            LEFT JOIN academic_projects ap ON ap.college_account_id = can.id
                AND ap.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                AND CONCAT(CURDATE(), ' 08:00:00')
            GROUP BY c.id, c.college_name
        """,
        "arena_count": """
            SELECT c.id AS college_id, c.college_name, COUNT(DISTINCT wc.id) AS arena_count
            FROM college c
            LEFT JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
            LEFT JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
            LEFT JOIN weekly_challenge wc ON wc.college_account_id = can.id
                AND wc.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                AND CONCAT(CURDATE(), ' 08:00:00')
            GROUP BY c.id, c.college_name
        """
    }
    
    final_df = base_df
    for metric, query in queries.items():
        df = execute_query(query)
        final_df = final_df.merge(df, on=["college_id", "college_name"], how="left")
    
    return final_df.fillna(0)

def get_assess_data():
    """Get ASSESS module data"""
    base_df = get_base_colleges()
    
    queries = {
        "objective_count": """
            SELECT c.id AS college_id, c.college_name, COUNT(DISTINCT q.id) AS objective_count
            FROM college c
            JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
            JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
            LEFT JOIN questionnaire q ON q.college_account_id = can.id
                AND q.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                AND CONCAT(CURDATE(), ' 08:00:00')
            GROUP BY c.id, c.college_name
        """,
        "subjective_count": """
            SELECT c.id AS college_id, c.college_name, COUNT(DISTINCT qs.id) AS subjective_count
            FROM college c
            JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
            JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
            LEFT JOIN questionnaire_subjective qs ON qs.college_account_id = can.id
                AND qs.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                AND CONCAT(CURDATE(), ' 08:00:00')
            GROUP BY c.id, c.college_name
        """,
        "coding_count": """
            SELECT c.id AS college_id, c.college_name, COUNT(DISTINCT ct.id) AS coding_count
            FROM college c
            LEFT JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
            LEFT JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
            LEFT JOIN coding_test ct ON ct.college_account_id = can.id
                AND ct.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), " 08:00:00"), INTERVAL 1 DAY)
                AND CONCAT(CURDATE(), " 08:00:00")
            GROUP BY c.id, c.college_name
        """
    }
    
    final_df = base_df
    for metric, query in queries.items():
        df = execute_query(query)
        final_df = final_df.merge(df, on=["college_id", "college_name"], how="left")
    
    return final_df.fillna(0)

def get_track_data():
    """Get TRACK module data"""
    base_df = get_base_colleges()
    
    queries = {
        "track_count": """
            SELECT c.id AS college_id, c.college_name, COUNT(DISTINCT casssat.college_id) AS track_count
            FROM college c
            LEFT JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
            LEFT JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
            LEFT JOIN college_account_subject_section_stats_activity_tracker casssat ON casssat.faculty_id = can.id
                AND casssat.faculty_mapping_start_date BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                AND CONCAT(CURDATE(), ' 08:00:00')
            GROUP BY c.id, c.college_name
        """,
        "semester_feedback_count": """
            SELECT c.id AS college_id, c.college_name, COUNT(DISTINCT sf.id) AS semester_feedback_count
            FROM college c
            LEFT JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
            LEFT JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
            LEFT JOIN semester_feedback sf ON sf.faculty_id = can.id
                AND sf.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                AND CONCAT(CURDATE(), ' 08:00:00')
            GROUP BY c.id, c.college_name
        """,
        "unit_feedback_count": """
            SELECT c.id AS college_id, c.college_name, COUNT(DISTINCT ff.id) AS unit_feedback_count
            FROM college c
            LEFT JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
            LEFT JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
            LEFT JOIN faculty_feedback ff ON ff.college_account_id = can.id
                AND ff.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
                AND CONCAT(CURDATE(), ' 08:00:00')
            GROUP BY c.id, c.college_name
        """
    }
    
    final_df = base_df
    for metric, query in queries.items():
        df = execute_query(query)
        final_df = final_df.merge(df, on=["college_id", "college_name"], how="left")
    
    return final_df.fillna(0)

def get_analyse_data():
    """Get ANALYSE module data"""
    base_df = get_base_colleges()
    
    query = """
        SELECT c.id AS college_id, c.college_name, COUNT(DISTINCT sfq.id) AS analyse_count
        FROM college c
        LEFT JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
        LEFT JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
        LEFT JOIN swoc_faculty_questionnaire sfq ON sfq.college_account_id = can.id
            AND sfq.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
            AND CONCAT(CURDATE(), ' 08:00:00')
        GROUP BY c.id, c.college_name
    """
    
    df = execute_query(query)
    final_df = base_df.merge(df, on=["college_id", "college_name"], how="left")
    return final_df.fillna(0)

def get_remediate_data():
    """Get REMEDIATE module data"""
    base_df = get_base_colleges()
    
    query = """
        SELECT c.id AS college_id, c.college_name, COUNT(DISTINCT qrp.id) AS remediate_count
        FROM college c
        LEFT JOIN college_university_degree_department_new cuddn ON cuddn.college_id = c.id
        LEFT JOIN college_account_new can ON can.college_university_degree_department_id = cuddn.id
        LEFT JOIN questionnaire q ON q.college_account_id = can.id
        LEFT JOIN questionnaire_remedial_path qrp ON qrp.questionnaire_id = q.id
            AND qrp.created_at BETWEEN DATE_SUB(CONCAT(CURDATE(), ' 08:00:00'), INTERVAL 1 DAY)
            AND CONCAT(CURDATE(), ' 08:00:00')
        GROUP BY c.id, c.college_name
    """
    
    df = execute_query(query)
    final_df = base_df.merge(df, on=["college_id", "college_name"], how="left")
    return final_df.fillna(0)

# ==========================================
# GENERATE REPORTS
# ==========================================

def generate_reports():
    """Generate individual and summary reports"""
    print("📊 Fetching data from database...")
    
    # Get all module data
    teach_df = get_teach_data()
    engage_df = get_engage_data()
    assess_df = get_assess_data()
    track_df = get_track_data()
    analyse_df = get_analyse_data()
    remediate_df = get_remediate_data()
    
    # Remove 'name' column if exists
    dfs = [teach_df, engage_df, assess_df, track_df, analyse_df, remediate_df]
    for df in dfs:
        if "name" in df.columns:
            df.drop(columns=["name"], inplace=True)
    
    # Merge all dataframes
    from functools import reduce
    combined_df = reduce(
        lambda left, right: pd.merge(left, right, on=["college_id", "college_name"], how="left"),
        dfs
    )
    combined_df.fillna(0, inplace=True)
    
    # Add college usage sum
    numeric_cols = combined_df.drop(columns=["college_id"], errors="ignore").select_dtypes(include=["number"])
    combined_df["college_usage"] = numeric_cols.sum(axis=1)
    
    # Add serial number
    combined_df.insert(0, "S.No", range(1, len(combined_df) + 1))
    
    # ===== SUMMARY REPORT =====
    dfs_dict = {
        "teach": teach_df,
        "engage": engage_df,
        "assess": assess_df,
        "track": track_df,
        "analyse": analyse_df,
        "remediate": remediate_df
    }
    
    result_df = teach_df[["college_id", "college_name"]].copy()
    
    for name, df in dfs_dict.items():
        numeric_cols = [c for c in df.select_dtypes(include="number").columns if c != "college_id"]
        df[name] = df[numeric_cols].sum(axis=1)
        result_df = result_df.merge(df[["college_id", name]], on="college_id", how="left")
    
    result_df.fillna(0, inplace=True)
    result_df["total"] = result_df[["teach", "engage", "assess", "track", "analyse", "remediate"]].sum(axis=1)
    result_df = result_df.sort_values(by="total", ascending=False).reset_index(drop=True)
    result_df.insert(0, "S.No", range(1, len(result_df) + 1))
    
    # Add total row
    total_row = {
        "S.No": "Total",
        "college_id": "-",
        "college_name": "Overall Total"
    }
    for col in ["teach", "engage", "assess", "track", "analyse", "remediate", "total"]:
        total_row[col] = result_df[col].sum()
    
    result_df = pd.concat([result_df, pd.DataFrame([total_row])], ignore_index=True)
    
    print("✅ Reports generated successfully")
    return combined_df, result_df

# ==========================================
# UPLOAD TO S3
# ==========================================

def upload_to_s3(combined_df, result_df):
    """Upload both sheets to S3 as single Excel file"""
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        combined_df.to_excel(writer, sheet_name="Individual Report", index=False)
        result_df.to_excel(writer, sheet_name="Summary Report", index=False)
    
    output.seek(0)
    
    s3 = boto3.client('s3')
    bucket_name = os.environ.get('S3_BUCKET', 'your-bucket-name')  # Set in Lambda env
    file_name = f"TEATER_Report_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=output.getvalue(),
        ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    
    print(f"✅ Uploaded to S3: s3://{bucket_name}/{file_name}")
    return bucket_name, file_name, output

# ==========================================
# SEND EMAIL VIA SES
# ==========================================

def send_email_with_attachment(bucket_name, file_name, file_buffer):
    """Send email with Excel attachment using AWS SES"""
    
    ses = boto3.client('ses', region_name=os.environ.get('AWS_REGION', 'ap-south-1'))
    
    # Email configuration
    sender = os.environ.get('SENDER_EMAIL', 'your-verified-email@example.com')
    recipients = os.environ.get('RECIPIENT_EMAILS', 'recipient@example.com').split(',')
    subject = f"Daily TEATER Report - {datetime.now().strftime('%d %B %Y')}"
    
    # Create message
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    
    # Email body
    body_html = f"""
    <html>
    <head></head>
    <body>
        <h2>Daily TEATER Usage Report</h2>
        <p>Dear Team,</p>
        <p>Please find attached the daily TEATER platform usage report for <strong>{(datetime.now() - timedelta(days=1)).strftime('%d %B %Y')}</strong>.</p>
        
        <h3>Report Contents:</h3>
        <ul>
            <li><strong>Individual Report:</strong> Detailed metrics for each college</li>
            <li><strong>Summary Report:</strong> Aggregated usage by module (Teach, Engage, Assess, Track, Analyse, Remediate)</li>
        </ul>
        
        <p>The report has also been uploaded to S3: <code>s3://{bucket_name}/{file_name}</code></p>
        
        <p>Best regards,<br>EdWisely Analytics Team</p>
    </body>
    </html>
    """
    
    body_text = f"""
    Daily TEATER Usage Report
    
    Dear Team,
    
    Please find attached the daily TEATER platform usage report for {(datetime.now() - timedelta(days=1)).strftime('%d %B %Y')}.
    
    Report Contents:
    - Individual Report: Detailed metrics for each college
    - Summary Report: Aggregated usage by module
    
    The report has also been uploaded to S3: s3://{bucket_name}/{file_name}
    
    Best regards,
    EdWisely Analytics Team
    """
    
    msg.attach(MIMEText(body_text, 'plain'))
    msg.attach(MIMEText(body_html, 'html'))
    
    # Attach Excel file
    attachment = MIMEApplication(file_buffer.getvalue())
    attachment.add_header('Content-Disposition', 'attachment', filename=file_name)
    msg.attach(attachment)
    
    # Send email
    try:
        response = ses.send_raw_email(
            Source=sender,
            Destinations=recipients,
            RawMessage={'Data': msg.as_string()}
        )
        print(f"✅ Email sent! Message ID: {response['MessageId']}")
        return response
    except Exception as e:
        print(f"❌ Email sending failed: {str(e)}")
        raise

# ==========================================
# LAMBDA HANDLER
# ==========================================

def lambda_handler(event, context):
    """Main Lambda handler"""
    try:
        print("🚀 Starting TEATER report generation...")
        
        # Generate reports
        combined_df, result_df = generate_reports()

        print('combine df ', combined_df, result_df)


        print("generating the output into excel sheets")
        # Generating the excel files....
        combined_df.to_excel("teater_individual_output.xlsx", index = False)
        result_df.to_excel("teater_usage_output.xlsx", index = False)
        with pd.ExcelWriter("OVERALL_TEATER_DAILY_USAGE.xlsx") as writer:
            result_df.to_excel(writer, sheet_name="Sheet1", index=False)
            combined_df.to_excel(writer, sheet_name="Sheet2", index=False)

        print("excel file generated successfully")
            
        # Upload to S3
        # bucket_name, file_name, file_buffer = upload_to_s3(combined_df, result_df)
        
        # # Send email
        # send_email_with_attachment(bucket_name, file_name, file_buffer)
        
        print("✅ Process completed successfully!")
        
        # return {
        #     "statusCode": 200,
        #     "body": {
        #         "message": "Report generated and sent successfully",
        #         "s3_location": f"s3://{bucket_name}/{file_name}",
        #         "report_date": datetime.now().strftime('%Y-%m-%d')
        #     }
        # }
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            "statusCode": 500,
            "body": {
                "error": str(e)
            }
        }

# For local testing
if __name__ == "__main__":
    lambda_handler(None, None)