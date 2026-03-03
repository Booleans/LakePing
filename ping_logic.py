import subprocess
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

def ping_host(ip: str) -> dict:
    result = subprocess.run(
        ["ping", "-c", "1", "-W", "2", ip],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    return {"ip": ip, "status": "UP" if result.returncode == 0 else "DOWN"}

def run_ping_sweep(spark) -> pd.DataFrame:
    # Load phone IPs from a Delta table on the cluster
    phones_df = spark.sql(
        "SELECT phone_id, ip_address FROM main.anicholls.phone_inventory"
    ).toPandas()

    phone_ips = phones_df["ip_address"].tolist()

    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = {executor.submit(ping_host, ip): ip for ip in phone_ips}
        results = [f.result() for f in as_completed(futures)]

    results_df = pd.DataFrame(results)
    return phones_df.merge(results_df, left_on="ip_address", right_on="ip")