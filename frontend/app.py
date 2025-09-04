import streamlit as st
import pandas as pd
import re
import datetime
from io import StringIO
import subprocess

st.set_page_config(page_title="Log Analysis UI", layout="wide")

# Regex to parse common Apache/NASA log format
LOG_RE = re.compile(
    r'^(?P<host>\S+) (?P<identity>\S+) (?P<user>\S+) \[(?P<time>.*?)\] "(?P<request>.*?)" (?P<status>\d{3}) (?P<size>\S+)'
)

def parse_log_lines(lines):
    rows = []
    for line in lines:
        m = LOG_RE.match(line)
        if not m:
            continue
        d = m.groupdict()
        # size
        size = 0 if d['size'] == '-' else int(d['size'])
        # parse datetime
        time_str = d['time']
        dt = None
        for fmt in ("%d/%b/%Y:%H:%M:%S %z", "%d/%b/%Y:%H:%M:%S"):
            try:
                dt = datetime.datetime.strptime(time_str, fmt)
                break
            except Exception:
                continue
        # request parts
        req_parts = d['request'].split()
        method = req_parts[0] if len(req_parts) >= 1 else None
        path = req_parts[1] if len(req_parts) >= 2 else d['request']
        protocol = req_parts[2] if len(req_parts) >= 3 else None

        row = {
            'host': d['host'],
            'identity': d['identity'],
            'user': d['user'],
            'time': time_str,
            'datetime': dt,
            'date': dt.date().isoformat() if dt else None,
            'month': dt.strftime("%Y-%m") if dt else None,
            'request': d['request'],
            'method': method,
            'path': path,
            'protocol': protocol,
            'status': int(d['status']),
            'size': size
        }
        rows.append(row)
    df = pd.DataFrame(rows)
    return df

# Analysis functions
def top_urls(df, n=10):
    if 'path' not in df.columns:
        return pd.DataFrame()
    res = df.groupby('path').size().reset_index(name='count').sort_values('count', ascending=False).head(n)
    return res

def requests_per_day(df):
    res = df.groupby('date').size().reset_index(name='count').sort_values('date')
    return res

def unique_users_per_month(df):
    res = df.groupby('month')['host'].nunique().reset_index(name='unique_hosts').sort_values('month')
    return res

def count_404s(df):
    return pd.DataFrame([{'404_count': int((df['status'] == 404).sum())}])

def top_hosts(df, n=10):
    res = df.groupby('host').size().reset_index(name='count').sort_values('count', ascending=False).head(n)
    return res

def avg_size_by_status(df):
    res = df.groupby('status')['size'].mean().reset_index(name='avg_size').sort_values('status')
    return res

# Try run hive query (PyHive if available, otherwise hive CLI)
def run_hive_query(sql, hive_host='localhost', hive_port=10000, username='chirag', database='log_analysis'):
    # Attempt PyHive
    try:
        from pyhive import hive
        conn = hive.Connection(host=hive_host, port=hive_port, username=username, database=database)
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except Exception as e:
        # Fallback to hive CLI
        try:
            cmd = ['hive', '-S', '-e', 'set hive.cli.print.header=true; {};'.format(sql)]
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('utf-8')
            return pd.read_csv(StringIO(out), sep='\t')
        except Exception as e2:
            st.error("Failed to run Hive query. PyHive error: {}. Hive CLI error: {}".format(e, e2))
            return pd.DataFrame()

#### UI ####
st.title("Log Data Analysis — Streamlit Frontend")
st.write("Upload a log file (Apache/NASA style) or connect to Hive `processed_logs` table.")

mode = st.radio("Analysis mode:", ["Local (upload file)", "Hive (query processed_logs)"])

if mode == "Local (upload file)":
    uploaded = st.file_uploader("Upload a log file", type=['log', 'txt'], accept_multiple_files=False)
    sample_btn = st.button("Use sample dataset from project data folder")
    df = pd.DataFrame()
    if uploaded is not None:
        text = uploaded.getvalue().decode('utf-8', errors='ignore')
        lines = text.splitlines()
        df = parse_log_lines(lines)
        st.success(f"Parsed {len(df)} log lines.")
    elif sample_btn:
        # attempt to read NASA sample file from project path
        try:
            sample_path = "/home/chirag/log-data-pipeline/data/NASA_access_log_Jul95"
            with open(sample_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            df = parse_log_lines(lines)
            st.success(f"Loaded sample file ({sample_path}) with {len(df)} parsed lines.")
        except Exception as e:
            st.error("Could not load sample dataset: " + str(e))
    else:
        st.info("Upload a log file or click 'Use sample dataset' to proceed.")

    if not df.empty:
        analyse = st.selectbox("Select analysis:", [
            "Top 10 URLs",
            "Requests per day",
            "Unique hosts per month",
            "Count 404 errors",
            "Top 10 hosts",
            "Average response size by status"
        ])
        if st.button("Run Analysis"):
            if analyse == "Top 10 URLs":
                out = top_urls(df, 20)
                st.dataframe(out)
                if not out.empty:
                    st.bar_chart(out.set_index('path')['count'])
            elif analyse == "Requests per day":
                out = requests_per_day(df)
                st.dataframe(out)
                if not out.empty:
                    st.line_chart(out.set_index('date')['count'])
            elif analyse == "Unique hosts per month":
                out = unique_users_per_month(df)
                st.dataframe(out)
                if not out.empty:
                    st.bar_chart(out.set_index('month')['unique_hosts'])
            elif analyse == "Count 404 errors":
                out = count_404s(df)
                st.dataframe(out)
            elif analyse == "Top 10 hosts":
                out = top_hosts(df, 20)
                st.dataframe(out)
                if not out.empty:
                    st.bar_chart(out.set_index('host')['count'])
            elif analyse == "Average response size by status":
                out = avg_size_by_status(df)
                st.dataframe(out)
            # allow download
            if 'out' in locals() and not out.empty:
                csv = out.to_csv(index=False).encode('utf-8')
                st.download_button("Download results CSV", csv, file_name="analysis_results.csv", mime="text/csv")

        # Option: upload file to HDFS for later Hive ingestion
        st.write("---")
        if st.button("Save uploaded file to project data folder"):
            try:
                save_path = "/home/chirag/log-data-pipeline/data/uploaded_log.log"
                if uploaded is not None:
                    with open(save_path, 'wb') as f:
                        f.write(uploaded.getvalue())
                    st.success(f"Saved to {save_path}")
                else:
                    st.error("No uploaded file found.")
            except Exception as e:
                st.error("Failed to save file: " + str(e))

        if st.button("Upload to Hive (HDFS + Load into processed_logs)"):
            try:
                save_path = "/home/chirag/log-data-pipeline/data/uploaded_log.log"

                # 1. Upload to HDFS
                subprocess.check_call(['hadoop', 'fs', '-put', '-f', save_path, '/user/chirag/raw_logs/'])
                st.success("Step 1: Uploaded file to HDFS (/user/chirag/raw_logs/)")

                # 2. Run Hive ingestion script
                hive_script_path = "/home/chirag/log-data-pipeline/hive_scripts/load_processed_table.sql"
                subprocess.check_call(['hive', '-f', hive_script_path])
                st.success("Step 2: Hive ingestion script executed. Data loaded into processed_logs.")

                # 3. Quick validation query
                sql = "SELECT COUNT(*) AS total_rows FROM processed_logs"
                cmd = ['hive', '-S', '-e', f"set hive.cli.print.header=true; {sql};"]
                out = subprocess.check_output(cmd).decode('utf-8')
                st.success(f"Step 3: processed_logs now contains:\n{out}")

            except subprocess.CalledProcessError as e:
                st.error(f"Command failed: {e}")
            except Exception as e:
                st.error(f"Unexpected error: {e}")


elif mode == "Hive (query processed_logs)":
    st.write("**Hive mode**: Run aggregated queries against `processed_logs` table in Hive.")
    hive_host = st.text_input("Hive host", value="localhost")
    hive_port = st.number_input("Hive port", value=10000)
    hive_user = st.text_input("Hive username", value="chirag")
    db_name = st.text_input("Database", value="log_analysis")

    hive_query = st.selectbox("Choose analysis (these queries read from processed_logs):", [
        "Top 10 most requested URLs",
        "Requests per day",
        "Unique users per month",
        "Count 404 (Not Found) errors",
        "Top 10 hosts",
        "Average response size by status"
    ])

    if st.button("Run Hive analysis"):
        sql_map = {
            "Top 10 most requested URLs": "SELECT path as request, COUNT(*) AS total_requests FROM processed_logs GROUP BY path ORDER BY total_requests DESC LIMIT 10",
            "Requests per day": "SELECT log_date as date, COUNT(*) AS daily_requests FROM processed_logs GROUP BY log_date ORDER BY log_date",
            "Unique users per month": "SELECT substr(log_date, 4,3) as month, COUNT(DISTINCT host) as unique_users FROM processed_logs GROUP BY substr(log_date,4,3) ORDER BY month",
            "Count 404 (Not Found) errors": "SELECT COUNT(*) as total_404s FROM processed_logs WHERE status = 404",
            "Top 10 hosts": "SELECT host, COUNT(*) as total_requests FROM processed_logs GROUP BY host ORDER BY total_requests DESC LIMIT 10",
            "Average response size by status": "SELECT status, AVG(size) as avg_response_size FROM processed_logs GROUP BY status ORDER BY status"
        }
        sql = sql_map[hive_query]
        st.write("Running SQL on Hive...")
        df = run_hive_query(sql, hive_host, hive_port, hive_user, db_name)
        if df is not None and not df.empty:
            st.dataframe(df)
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button("Download results CSV", csv, file_name="hive_results.csv", mime="text/csv")
        else:
            st.info("No results or query failed. Check Hive connection and that table `processed_logs` exists.")
