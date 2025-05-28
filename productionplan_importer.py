import pandas as pd
from sqlalchemy import text , create_engine
from datetime import datetime, timedelta
from config import EXCEL_FILE_PATH, SHEET_DATA_GUIDE ,DB_URL

def import_productionplan():
    def get_engine():
        return create_engine(DB_URL, pool_recycle=300)
    engine = get_engine()
    connection = engine.connect()
    xls = pd.read_excel(EXCEL_FILE_PATH, sheet_name=None)
    df_plan = xls.get(SHEET_DATA_GUIDE, pd.DataFrame())
    product_df = pd.read_sql("SELECT id, name, project_id FROM product", connection)
    product_map = {row["name"].strip(): {"id": row["id"], "project_id": row["project_id"]} for _, row in product_df.iterrows()}
    process_order_id = connection.execute(text("SELECT id FROM processorder LIMIT 1")).scalar()
    if not process_order_id:
        raise ValueError("No process_order found in the table!")
    def set_end_time_exclusive(start, delta):
        return start + delta - timedelta(seconds=1)
    def get_base_hierarchy(hierarchy):
        parts = hierarchy.split("$")
        if parts and parts[-1].startswith("ast_"):
            parts = parts[:-1]
        return "$".join(parts)
    grouped = {}
    for idx, row in df_plan.iterrows():
        name = str(row.get("name", "")).strip()
        hierarchy = str(row.get("hierarchy", "")).strip()
        if not name or not hierarchy or pd.isna(row.get("planned_quantity")):
            continue
        base_hier = get_base_hierarchy(hierarchy)
        grouped.setdefault(base_hier, []).append((idx, row))
    base_start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    inserted = 0
    duplicates = 0
    skipped = 0
    try:
        for base_hier, rows in grouped.items():
            start_time = base_start_time
            for idx, row in rows:
                name = str(row.get("name", "")).strip()
                hierarchy = str(row.get("hierarchy", "")).strip()
                planned_quantity = row.get("planned_quantity")
                duration_hrs = row.get("Duration_hrs")
                duration_type = str(row.get("type", "")).strip().lower()
                if name not in product_map:
                    print(f"[SKIPPED] Product '{name}' not found in product table.")
                    skipped += 1
                    continue
                product_id = product_map[name]["id"]
                project_id = product_map[name]["project_id"]
                # Special handling for week
                if duration_type == "week":
                    # Always start from the most recent Monday
                    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                    week_start = today - timedelta(days=today.weekday())
                    plan_intervals = [
                        (week_start, week_start + timedelta(days=2) - timedelta(seconds=1)),
                        (week_start + timedelta(days=2), week_start + timedelta(days=4) - timedelta(seconds=1)),
                        (week_start + timedelta(days=4), week_start + timedelta(days=6) - timedelta(seconds=1)),
                    ]
                    for current_start, current_end in plan_intervals:
                        exists = connection.execute(text("""
                            SELECT 1 FROM productionplan
                            WHERE product = :pid AND hierarchy = :hier
                            AND end_time >= :start_time AND start_time <= :end_time
                        """), {'pid': product_id, 'hier': hierarchy, 'start_time': current_start, 'end_time': current_end}).fetchone()
                        if exists:
                            print(f"[OVERLAP] {name} (hierarchy: {hierarchy}) overlaps existing plan in {current_start} to {current_end}. Skipping insert.")
                            duplicates += 1
                            continue
                        try:
                            connection.execute(text("""
                                INSERT INTO productionplan (
                                    project_id, meta, hierarchy, product, process_order,
                                    start_time, end_time, planned_quantity,
                                    oee_target, performance_target,
                                    availability_target, quality_target
                                ) VALUES (
                                    :project_id, NULL, :hierarchy, :product, :process_order,
                                    :start_time, :end_time, :planned_quantity,
                                    100, 100, 100, 100
                                )
                            """), {
                                "project_id": project_id,
                                "hierarchy": hierarchy,
                                "product": product_id,
                                "process_order": process_order_id,
                                "start_time": current_start,
                                "end_time": current_end,
                                "planned_quantity": planned_quantity,
                            })
                            print(f"[INSERTED] {name} (hierarchy: {hierarchy}) week interval from {current_start} to {current_end}")
                            inserted += 1
                        except Exception as e:
                            print(f"[ERROR] {name} at row {idx}: {e}")
                            skipped += 1
                    continue
                # Special handling for month
                elif duration_type == "month":
                    start_time = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    duration = timedelta(hours=float(duration_hrs))
                    current_start = start_time
                    current_end = set_end_time_exclusive(current_start, duration)
                    exists = connection.execute(text("""
                        SELECT 1 FROM productionplan
                        WHERE product = :pid AND hierarchy = :hier
                        AND end_time >= :start_time AND start_time <= :end_time
                    """), {'pid': product_id, 'hier': hierarchy, 'start_time': current_start, 'end_time': current_end}).fetchone()
                    if exists:
                        print(f"[DUPLICATE] {name} (hierarchy: {hierarchy}) already exists for month period.")
                        duplicates += 1
                        continue
                    try:
                        connection.execute(text("""
                            INSERT INTO productionplan (
                                project_id, meta, hierarchy, product, process_order,
                                start_time, end_time, planned_quantity,
                                oee_target, performance_target,
                                availability_target, quality_target
                            ) VALUES (
                                :project_id, NULL, :hierarchy, :product, :process_order,
                                :start_time, :end_time, :planned_quantity,
                                100, 100, 100, 100
                            )
                        """), {
                            "project_id": project_id,
                            "hierarchy": hierarchy,
                            "product": product_id,
                            "process_order": process_order_id,
                            "start_time": current_start,
                            "end_time": current_end,
                            "planned_quantity": planned_quantity,
                        })
                        print(f"[INSERTED] {name} (hierarchy: {hierarchy}) month from {current_start} to {current_end}")
                        inserted += 1
                    except Exception as e:
                        print(f"[ERROR] {name} at row {idx}: {e}")
                        skipped += 1
                    continue
                # Default: hour/day
                else:
                    if duration_type == "hour":
                        duration = timedelta(hours=float(duration_hrs))
                        current_start = start_time
                        current_end = set_end_time_exclusive(current_start, duration)
                        start_time = current_end + timedelta(seconds=1)
                    elif duration_type == "day":
                        duration = timedelta(hours=float(duration_hrs))
                        current_start = base_start_time
                        current_end = set_end_time_exclusive(current_start, duration)
                    else:
                        # fallback: treat as hours
                        duration = timedelta(hours=float(duration_hrs))
                        current_start = start_time
                        current_end = set_end_time_exclusive(current_start, duration)
                        start_time = current_end + timedelta(seconds=1)
                    exists = connection.execute(text("""
                        SELECT 1 FROM productionplan
                        WHERE product = :pid AND hierarchy = :hier
                        AND end_time >= :start_time AND start_time <= :end_time
                    """), {'pid': product_id, 'hier': hierarchy, 'start_time': current_start, 'end_time': current_end}).fetchone()
                    if exists:
                        print(f"[DUPLICATE] {name} (hierarchy: {hierarchy}) already exists.")
                        duplicates += 1
                    else:
                        try:
                            connection.execute(text("""
                                INSERT INTO productionplan (
                                    project_id, meta, hierarchy, product, process_order,
                                    start_time, end_time, planned_quantity,
                                    oee_target, performance_target,
                                    availability_target, quality_target
                                ) VALUES (
                                    :project_id, NULL, :hierarchy, :product, :process_order,
                                    :start_time, :end_time, :planned_quantity,
                                    100, 100, 100, 100
                                )
                            """), {
                                "project_id": project_id,
                                "hierarchy": hierarchy,
                                "product": product_id,
                                "process_order": process_order_id,
                                "start_time": current_start,
                                "end_time": current_end,
                                "planned_quantity": planned_quantity,
                            })
                            print(f"[INSERTED] {name} (hierarchy: {hierarchy}) from {current_start} to {current_end}")
                            inserted += 1
                        except Exception as e:
                            print(f"[ERROR] {name} at row {idx}: {e}")
                            skipped += 1

    except Exception as e:
        connection.rollback()
        print(f"\n Production Plan Import failed due to an error: {e}")
        raise # Re-raise the exception after rollback
    else:
        connection.commit()
        print("\n Import Summary:")
        print(f"  Inserted:   {inserted}")
        print(f"  Duplicates: {duplicates}")
        print(f"  Skipped:    {skipped}")
