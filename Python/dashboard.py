from flask import Flask, render_template_string
import mysql.connector
import json

app = Flask(__name__)

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>📊 Network Monitoring Dashboard</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    body { padding: 30px; background: #f4f6f8; font-family: sans-serif; }
    h1 { margin-bottom: 30px; }
    pre { background: #f8f9fa; padding: 10px; border-radius: 5px; font-size: 12px; }
  </style>
</head>
<body>
  <div class="container">
    <h1>📡 Network Metrics (Last 10 Records)</h1>
    <table class="table table-bordered table-striped table-hover">
      <thead class="table-dark">
        <tr>
          <th>ID</th>
          <th>Host</th>
          <th>CPU %</th>
          <th>Memory %</th>
          <th>Net Connections</th>
          <th>Timestamp</th>
        </tr>
      </thead>
      <tbody>
        {% for row in data %}
        <tr>
          <td>{{ row.id }}</td>
          <td>{{ row.host }}</td>
          <td>{{ row.cpu_percent }}</td>
          <td>{{ row.memory_percent }}</td>
          <td>
            <button class="btn btn-sm btn-outline-primary" onclick="toggle('conn-{{ row.id }}')">Show</button>
            <pre id="conn-{{ row.id }}" style="display:none;">
{% for conn in row.net_connections %}
{{ conn.protocol }} | {{ conn.local_address }} -> {{ conn.foreign_address }} | {{ conn.state }} | PID: {{ conn.pid }} | Process: {{ conn.process }}
{% endfor %}
            </pre>
          </td>
          <td>{{ row.timestamp }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>

  <script>
    function toggle(id) {
      const el = document.getElementById(id);
      el.style.display = el.style.display === "none" ? "block" : "none";
    }
  </script>
</body>
</html>
'''

@app.route("/")
def dashboard():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Aakash10",
        database="monitoring"
    )
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM metrics ORDER BY id DESC LIMIT 10")
    rows = cursor.fetchall()

    # Parse net_connections
    for row in rows:
        try:
            row["net_connections"] = json.loads(row["net_connections"])
        except Exception:
            row["net_connections"] = []

    return render_template_string(HTML_TEMPLATE, data=rows)

if __name__ == "__main__":
    app.run(port=5000)
