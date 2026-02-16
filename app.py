
# WorkNest Mini v3.3.0
# Unified Alerts + Office Chat

def get_user_alerts(user_id, conn):
    alerts = []
    cur = conn.cursor()

    # Overdue tasks
    cur.execute("""
        SELECT t.title, t.due_date
        FROM task_assignments ta
        JOIN tasks t ON t.id = ta.task_id
        WHERE ta.staff_id = %s
        AND ta.status != 'Completed'
        AND t.due_date < CURRENT_DATE
    """, (user_id,))
    for row in cur.fetchall():
        alerts.append({
            "severity": "high",
            "message": f"Overdue task: {row[0]}"
        })

    # Due soon
    cur.execute("""
        SELECT t.title, t.due_date
        FROM task_assignments ta
        JOIN tasks t ON t.id = ta.task_id
        WHERE ta.staff_id = %s
        AND ta.status != 'Completed'
        AND t.due_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '3 days'
    """, (user_id,))
    for row in cur.fetchall():
        alerts.append({
            "severity": "medium",
            "message": f"Task due soon: {row[0]}"
        })

    return alerts


CHAT_SCHEMA = """
CREATE TABLE IF NOT EXISTS chat_messages (
    id SERIAL PRIMARY KEY,
    staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL,
    message TEXT,
    image_path TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
