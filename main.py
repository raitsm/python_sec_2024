import psutil
import socket
import pandas as pd
import time
from datetime import datetime


def monitor_connections(duration):
    """
    Monitor network connections for a specified duration.
    
    Args:
    - duration (int): Time in seconds for how long the script should monitor connections.

    Returns:
    - connection_data (list): List of connections with metadata including start and end times.
    """
    start_time = datetime.now()
    connection_tracker = {}  # Track connections with start and end times

    print(f"Monitoring connections for {duration} seconds...")
    end_time = start_time + pd.Timedelta(seconds=duration)

    while datetime.now() < end_time:
        connections = psutil.net_connections(kind='inet')
        for conn in connections:
            try:
                laddr = conn.laddr  # Local address
                raddr = conn.raddr if conn.raddr else ("N/A", "N/A")
                protocol = "TCP" if conn.type == socket.SOCK_STREAM else "UDP"
                direction = "Outgoing" if raddr[0] != "N/A" else "Incoming"
                process = psutil.Process(conn.pid)
                process_name = process.name()
                process_path = process.exe()

                connection_key = (
                    f"{laddr.ip}:{laddr.port}",
                    f"{raddr[0]}:{raddr[1]}",
                    protocol
                )

                # Add new connections or update existing ones
                if connection_key not in connection_tracker:
                    connection_tracker[connection_key] = {
                        "Local Address": f"{laddr.ip}:{laddr.port}",
                        "Remote Address": f"{raddr[0]}:{raddr[1]}",
                        "Direction": direction,
                        "Protocol": protocol,
                        "Process Name": process_name,
                        "Process Path": process_path,
                        "Start Time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "End Time": None,
                    }
                else:
                    # Update end time to None for ongoing connections
                    connection_tracker[connection_key]["End Time"] = None
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                pass  # Skip connections we cannot access

        time.sleep(1)  # Poll connections every 1 second

    # Mark remaining connections as "continued"
    for conn_data in connection_tracker.values():
        if conn_data["End Time"] is None:
            conn_data["End Time"] = "continued"

    # Convert tracker to list
    return list(connection_tracker.values())


if __name__ == "__main__":
    duration_seconds = 60  # Example: Run for 10 seconds
    connections = monitor_connections(duration_seconds)

    # Save to CSV
    df = pd.DataFrame(connections)
    csv_file_path = "./connections_with_timestamps.csv"
    df.to_csv(csv_file_path, encoding="utf-8", index=False)
    print(f"Data saved to {csv_file_path}")
