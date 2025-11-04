import pandas as pd
from io import BytesIO
import boto3
from sqlalchemy import create_engine
import os
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

import smtplib
from email.message import EmailMessage

# ==========================================
# LOAD DATABASE CREDENTIALS SECURELY
# ==========================================
# Try to get from environment (GitHub Secrets)
DB_USER = os.getenv("DB_USER", "prod-read-user")
DB_PASS = os.getenv("DB_PASS", 'UY8C&"W>&A6I*g$WTCbb50rn')
DB_HOST = os.getenv("DB_HOST", "proddb-read-replica.crhg7zleeuhf.ap-south-1.rds.amazonaws.com")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "edwisely_college")

print(f"Connecting to database: {DB_NAME} at {DB_HOST}")
# ==========================================
# CREATE DATABASE CONNECTION
# ==========================================
try:
    # SQLAlchemy connection string
    connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)

    # Test connection
    with engine.connect() as connection:
        print("✅ Database connection successful!")

except Exception as e:
    print("❌ Database connection failed:", e)



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
# excel to pivot table
# ==========================================
def excel_to_pivot(result_df, combined_df):

    import pandas as pd
    from io import BytesIO
    import smtplib
    from email.message import EmailMessage

    output = BytesIO()

    # 🧾 Create Excel with summary and pivot
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        result_df.to_excel(writer, sheet_name="Usage_Data", index=False)
        combined_df.to_excel(writer, sheet_name="Individual_Data", index=False)

        workbook = writer.book
        worksheet = writer.sheets["Usage_Data"]
        worksheet.autofilter(0, 0, result_df.shape[0], result_df.shape[1] - 1)
        worksheet.freeze_panes(1, 1)
        worksheet.set_column("A:A", 40)
        worksheet.set_column("B:H", 12)

    output.seek(0)
    
    from datetime import datetime, timedelta
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    start_date = yesterday.strftime('%d-%b-%Y (08:00 AM)')
    end_date = today.strftime('%d-%b-%Y (08:00 AM)')
    
    styled_html = result_df.to_html(index=False, border=0, classes='styled-table')
    
    html_content = f"""
    <html>
      <head>
        <style>
          body {{
            font-family: 'Segoe UI', Arial, sans-serif;
            background-color: #f4f4f4;
            padding: 0;
            margin: 0;
          }}
          .container {{
            max-width: 600px;
            margin: 20px auto;
            background: #ffffff;
            border-radius: 10px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.08);
            overflow-x: auto;
          }}
          .header {{
            background-color: #007BFF;
            color: #fff;
            padding: 15px;
            border-radius: 10px 10px 0 0;
            text-align: center;
            font-size: 18px;
            font-weight: bold;
          }}
          .content {{
            padding: 20px;
          }}
          table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
          }}
          th, td {{
            border: 1px solid #ddd;
            padding: 10px;
            text-align: left;
            font-size: 14px;
          }}
          th {{
            background-color: #007BFF;
            color: white;
          }}
          tr:nth-child(even) {{
            background-color: #f9f9f9;
          }}
          .footer {{
            text-align: center;
            padding: 15px;
            font-size: 13px;
            color: #555;
          }}
          /* ✅ Make responsive for mobile */
          @media only screen and (max-width: 600px) {{
            body, .container {{
              width: 100% !important;
              margin: 0 !important;
            }}
            table, th, td {{
              font-size: 12px !important;
              display: block;
              width: 100% !important;
            }}
            th {{
              background-color: #007BFF;
              color: #fff;
            }}
            td {{
              border: none;
              border-bottom: 1px solid #eee;
            }}
          }}
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">📊 Daily Feature Utility Summary</div>
          <div class="content">
            <p>Hi Sir,</p>
    
            <p>I hope this message finds you well.</p>
    
            <p>
              Please find below the latest <strong>feature utility pivot table and chart summary</strong>,
              automatically generated from Google Sheets.
            </p>
    
            <p>
              <em>
                This data reflects the usage activity from <strong>03-Nov-2025 (08:00 AM)</strong>
                to <strong>04-Nov-2025 (08:00 AM)</strong>.
              </em>
            </p>
    
            <p>Kindly review the insights presented below:</p>
    
            {table_html}  <!-- Your dynamic table here -->
    
            <p style="margin-top: 20px;">Warm regards,</p>
            <p>
              <strong>Sridhar Goudu</strong><br>
              Engineering Solutions Analyst<br>
              Edwisely
            </p>
          </div>
          <div class="footer">
            <p>Automated Daily Insights Email • Generated via Python</p>
          </div>
        </div>
      </body>
    </html>
    """

    # ✅ Prepare the email
    EMAIL_USER = os.getenv("EMAIL_USER", "sridhar@edwisely.com")
    EMAIL_PASS = os.getenv("EMAIL_PASS", "yzcikxbrmpvvavsb")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "📈 Daily TEATER Usage Report"
    msg["From"] = "sridhar@edwisely.com"
    msg["To"] = "s190204@rguktsklm.ac.in"
    # msg["Cc"] = "narsimha@edwisely.com"

    # Attach the HTML body
    msg.attach(MIMEText(html_content, "html"))

    # Attach Excel file
    part = MIMEApplication(output.read(), _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    part.add_header('Content-Disposition', 'attachment', filename="OVERALL_TEATER_DAILY_USAGE.xlsx")
    msg.attach(part)

    # ✅ Send via Gmail SMTP
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(EMAIL_USER, EMAIL_PASS)
            smtp.send_message(msg)
        print("✅ Email with embedded table and Excel attachment sent successfully!")
    except Exception as e:
        print("❌ Email send failed:", e)

    
    # ==========================================
# LAMBDA HANDLER
# ==========================================

def teater_generation():
    """Main Lambda handler"""
    try:
        print("🚀 Starting TEATER report generation...")
        
        # Generate reports
        combined_df, result_df = generate_reports()

        print('combine df ', combined_df, result_df)


        print("generating the output into excel sheets")
        excel_to_pivot(result_df, combined_df)
        print("pivot_table converted successfully")
        

        #sending to mail
        print("✅ Process completed successfully!")
    

        
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
    teater_generation()










