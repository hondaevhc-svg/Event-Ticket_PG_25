import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
import io

# -------------------------------------------------
# BASIC CONFIG
# -------------------------------------------------
st.set_page_config(page_title="🎟️ Event Management System", layout="wide")

# CSS: Center align table content and force header styling
st.markdown("""
    <style>
    [data-testid="stTable"] td, [data-testid="stTable"] th {
        text-align: center !important;
    }
    div[data-testid="stDataFrame"] div[class^="st-"] {
        text-align: center !important;
    }
    .stDataFrame th {
        text-align: center !important;
    }
    /* Style for the Total Row to make it stand out */
    .stDataFrame tr:last-child {
        font-weight: bold;
        background-color: #f0f2f6;
    }
    </style>
""", unsafe_allow_html=True)

# -------------------------------------------------
# HELPERS
# -------------------------------------------------
def _get_password(key: str) -> str | None:
    return (
        st.secrets.get("app_passwords", {}).get(key)
        if "app_passwords" in st.secrets
        else os.getenv(key.upper())
    )

ADMIN_RESET_PASSWORD = _get_password("admin_reset") or _get_password("admin")
MENU_UPDATE_PASSWORD = _get_password("menu_update") or _get_password("admin")

def now_ts() -> str:
    return pd.Timestamp.now(tz="UTC").isoformat()

def to_excel_download(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

# -------------------------------------------------
# DB CONNECTION & CACHED LOAD
# -------------------------------------------------
@st.cache_resource
def get_engine():
    db_url = st.secrets["connections"]["postgresql"]["url"]
    return create_engine(db_url, pool_pre_ping=True, pool_recycle=1800)

@st.cache_data(ttl=60, show_spinner=False)
def load_all_data():
    engine = get_engine()
    tickets_df = pd.read_sql("SELECT * FROM tickets", engine)
    menu_df = pd.read_sql("SELECT * FROM menu", engine)

    column_map = {col: col.strip() for col in tickets_df.columns}
    tickets_df = tickets_df.rename(columns=column_map)
    
    if tickets_df.empty:
        tickets_df = pd.DataFrame(columns=["TicketID", "Category", "Type", "Admit", "Seq", "Sold", "Visited", "Customer", "Visitor_Seats", "Timestamp"])
        return tickets_df, menu_df
    
    # Data Cleaning
    tickets_df["Visitor_Seats"] = pd.to_numeric(tickets_df.get("Visitor_Seats", 0), errors="coerce").fillna(0).astype(int)
    tickets_df["Sold"] = tickets_df.get("Sold", False).fillna(False).astype(bool)
    tickets_df["Visited"] = tickets_df.get("Visited", False).fillna(False).astype(bool)
    tickets_df["Admit"] = pd.to_numeric(tickets_df.get("Admit", 1), errors="coerce").fillna(1).astype(int)
    tickets_df["TicketID"] = tickets_df["TicketID"].astype(str).str.zfill(4)
    
    return tickets_df, menu_df

def save_tickets_df(tickets_df: pd.DataFrame):
    engine = get_engine()
    with engine.begin() as conn:
        tickets_df.to_sql("tickets", con=conn, if_exists="replace", index=False, method="multi", chunksize=1000)
    st.cache_data.clear()

def save_both(tickets_df: pd.DataFrame, menu_df: pd.DataFrame):
    engine = get_engine()
    with engine.begin() as conn:
        tickets_df.to_sql("tickets", con=conn, if_exists="replace", index=False, method="multi", chunksize=1000)
        menu_df.to_sql("menu", con=conn, if_exists="replace", index=False, method="multi", chunksize=1000)
    st.cache_data.clear()

def custom_sort(df: pd.DataFrame) -> pd.DataFrame:
    if "Seq" not in df.columns or df.empty: return df
    sort_key = df["Seq"].apply(lambda x: float("inf") if pd.isna(x) or x in [0, "0"] else float(x))
    return df.assign(_k=sort_key).sort_values("_k").drop(columns="_k")

# Initial Load
try:
    tickets, menu = load_all_data()
except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    tickets = pd.DataFrame(columns=["TicketID", "Category", "Type", "Admit", "Seq", "Sold", "Visited", "Customer", "Visitor_Seats", "Timestamp"])
    menu = pd.DataFrame()

# -------------------------------------------------
# SIDEBAR
# -------------------------------------------------
with st.sidebar:
    st.header("Admin Settings")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    admin_pass_input = st.text_input("Reset Database Password", type="password")
    if st.button("🚨 Reset Database", use_container_width=True):
        if admin_pass_input == ADMIN_RESET_PASSWORD:
            tickets.loc[:, ["Sold", "Visited"]] = False
            tickets.loc[:, ["Customer"]] = ""
            tickets.loc[:, ["Visitor_Seats"]] = 0
            tickets.loc[:, ["Timestamp"]] = None
            save_tickets_df(tickets)
            st.success("✅ Database reset.")
            st.rerun()
        else:
            st.error("❌ Incorrect Password")

# -------------------------------------------------
# TABS
# -------------------------------------------------
tabs = st.tabs(["📊 Dashboard", "💰 Sales", "🚶 Visitors", "⚙️ Edit Menu"])

# --- 1. DASHBOARD ---
with tabs[0]:
    st.subheader("Inventory & Visitor Analytics")
    if tickets.empty:
        st.info("No tickets found.")
    else:
        summary = tickets.groupby(["Seq", "Type", "Category", "Admit"], dropna=False).agg(
            Total_Tickets=("TicketID", "count"),
            Tickets_Sold=("Sold", "sum"),
            Total_Visitors=("Visitor_Seats", "sum"),
        ).reset_index()
        summary["Total_Seats"] = summary["Total_Tickets"] * summary["Admit"]
        summary["Seats_sold"] = summary["Tickets_Sold"] * summary["Admit"]
        summary["Balance_Tickets"] = summary["Total_Tickets"] - summary["Tickets_Sold"]
        summary["Balance_Seats"] = summary["Total_Seats"] - summary["Seats_sold"]
        summary["Balance_Visitors"] = summary["Seats_sold"] - summary["Total_Visitors"]
        summary = custom_sort(summary)

        # Add Total Row
        total_row = pd.DataFrame({
            "Seq": ["TOTAL"], "Type": [""], "Category": [""], "Admit": [""],
            "Total_Tickets": [summary["Total_Tickets"].sum()],
            "Tickets_Sold": [summary["Tickets_Sold"].sum()],
            "Total_Visitors": [summary["Total_Visitors"].sum()],
            "Total_Seats": [summary["Total_Seats"].sum()],
            "Seats_sold": [summary["Seats_sold"].sum()],
            "Balance_Tickets": [summary["Balance_Tickets"].sum()],
            "Balance_Seats": [summary["Balance_Seats"].sum()],
            "Balance_Visitors": [summary["Balance_Visitors"].sum()]
        })
        summary_final = pd.concat([summary, total_row], ignore_index=True)
        
        # Display with height to ensure scrollability
        st.dataframe(summary_final, hide_index=True, use_container_width=True, height=500)

# --- 2. SALES ---
with tabs[1]:
    st.subheader("Sales Management")
    col_in, col_out = st.columns([1, 1.2])
    with col_in:
        sale_tab = st.radio("Action", ["Manual", "Bulk Upload", "Reverse Sale"], horizontal=True)
        
        if sale_tab == "Manual":
            s_type = st.radio("Type", ["Public", "Guest"], horizontal=True)
            s_cat_options = menu.loc[menu["Type"] == s_type, "Category"].dropna().unique().tolist()
            s_cat = st.selectbox("Category", s_cat_options)
            avail = tickets[(tickets["Type"] == s_type) & (tickets["Category"] == s_cat) & (~tickets["Sold"])]["TicketID"].tolist()
            if avail:
                with st.form("sale_form", clear_on_submit=True):
                    tid = st.selectbox("Ticket ID", avail)
                    cust = st.text_input("Customer Name")
                    if st.form_submit_button("Confirm Sale"):
                        idx = tickets.index[tickets["TicketID"] == tid][0]
                        tickets.at[idx, "Sold"] = True
                        tickets.at[idx, "Customer"] = cust
                        tickets.at[idx, "Timestamp"] = now_ts()
                        save_tickets_df(tickets)
                        st.success(f"✅ Ticket {tid} sold.")
                        st.rerun()
            else: st.info("No tickets available.")

        elif sale_tab == "Bulk Upload":
            st.info("📋 Columns: `Ticket_ID`, `Customer`")
            uploaded_file = st.file_uploader("Upload File", type=["csv", "xlsx"], key="sale_bulk")
            if uploaded_file:
                bulk_df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith(".csv") else pd.read_excel(uploaded_file)
                if {"Ticket_ID", "Customer"}.issubset(bulk_df.columns):
                    bulk_df["Ticket_ID"] = bulk_df["Ticket_ID"].astype(str).str.zfill(4)
                    
                    # 1) Cross-reference against Recent Sales (already Sold)
                    already_sold_mask = bulk_df["Ticket_ID"].isin(tickets[tickets["Sold"]]["TicketID"])
                    already_sold_list = bulk_df[already_sold_mask]
                    valid_to_sell = bulk_df[~already_sold_mask]

                    if not already_sold_list.empty:
                        st.warning(f"⚠️ {len(already_sold_list)} Tickets are already in Sales History.")
                        st.download_button("📥 Download 'Already Sold' List", 
                                         data=to_excel_download(already_sold_list), 
                                         file_name="already_sold_tickets.xlsx")

                    if st.button("Process Valid Bulk Sales"):
                        id_to_index = {tid: i for i, tid in enumerate(tickets["TicketID"].tolist())}
                        count = 0
                        for _, row in valid_to_sell.iterrows():
                            tid = row["Ticket_ID"]
                            if tid in id_to_index:
                                idx = id_to_index[tid]
                                tickets.at[idx, "Sold"] = True
                                tickets.at[idx, "Customer"] = row["Customer"]
                                tickets.at[idx, "Timestamp"] = now_ts()
                                count += 1
                        save_tickets_df(tickets)
                        st.success(f"✅ {count} Sales Processed.")
                        st.rerun()
                else: st.error("Invalid Columns.")

        elif sale_tab == "Reverse Sale":
            r_tid = st.text_input("Enter Ticket ID to reverse")
            if st.button("Reverse"):
                idx_list = tickets.index[tickets["TicketID"] == r_tid.zfill(4)].tolist()
                if idx_list:
                    idx = idx_list[0]
                    tickets.at[idx, "Sold"] = False
                    tickets.at[idx, "Customer"] = ""
                    tickets.at[idx, "Visited"] = False
                    tickets.at[idx, "Visitor_Seats"] = 0
                    save_tickets_df(tickets)
                    st.success("✅ Sale Reversed.")
                    st.rerun()

    with col_out:
        st.write("**Recent Sales History**")
        # Removed .head(10) and added height for full record viewing via scroll
        st.dataframe(tickets[tickets["Sold"]].sort_values("Timestamp", ascending=False), 
                     hide_index=True, use_container_width=True, height=500)

# --- 3. VISITORS ---
with tabs[2]:
    st.subheader("Visitor Entry Management")
    v_in, v_out = st.columns([1, 1.2])

    with v_in:
        v_action = st.radio("Action", ["Entry", "Bulk Upload", "Reverse Entry"], horizontal=True)

        if v_action == "Entry":
            v_type = st.radio("Entry Type", ["Public", "Guest"], horizontal=True)
            v_cat_options = menu.loc[menu["Type"] == v_type, "Category"].dropna().unique().tolist()
            v_cat = st.selectbox("Entry Category", v_cat_options)
            elig = tickets[(tickets["Type"] == v_type) & (tickets["Category"] == v_cat) & (tickets["Sold"]) & (~tickets["Visited"])]["TicketID"].tolist()
            
            if elig:
                with st.form("checkin_form"):
                    tid = st.selectbox("Select Ticket ID", elig)
                    max_v = int(tickets.loc[tickets["TicketID"] == tid, "Admit"].values[0])
                    v_count = st.number_input("Confirmed Visitors", min_value=1, max_value=max_v, value=max_v)
                    if st.form_submit_button("Confirm Entry"):
                        idx = tickets.index[tickets["TicketID"] == tid][0]
                        tickets.at[idx, "Visited"] = True
                        tickets.at[idx, "Visitor_Seats"] = int(v_count)
                        tickets.at[idx, "Timestamp"] = now_ts()
                        save_tickets_df(tickets)
                        st.success(f"✅ Entry confirmed.")
                        st.rerun()
            else: st.info("No eligible (sold & unvisited) tickets found.")

        elif v_action == "Bulk Upload":
            st.info("📋 Columns: `Ticket_ID`, `Visitor_Count`. Only SOLD tickets allowed.")
            uploaded_file = st.file_uploader("Choose File", type=["csv", "xlsx"], key="vis_bulk")
            if uploaded_file:
                bulk_df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith(".csv") else pd.read_excel(uploaded_file)
                if {"Ticket_ID", "Visitor_Count"}.issubset(bulk_df.columns):
                    bulk_df["Ticket_ID"] = bulk_df["Ticket_ID"].astype(str).str.zfill(4)
                    
                    # Logic: Only allow 'Sold' tickets
                    sold_ids = tickets[tickets["Sold"]]["TicketID"].tolist()
                    unsold_mask = ~bulk_df["Ticket_ID"].isin(sold_ids)
                    
                    unsold_list = bulk_df[unsold_mask]
                    valid_to_entry = bulk_df[~unsold_mask]

                    if not unsold_list.empty:
                        st.error(f"❌ {len(unsold_list)} Tickets have NOT been sold yet.")
                        st.download_button("📥 Download 'Unsold' List", 
                                         data=to_excel_download(unsold_list), 
                                         file_name="unsold_visitor_attempts.xlsx")

                    if st.button("Process Valid Visitor Upload"):
                        id_to_index = {tid: i for i, tid in enumerate(tickets["TicketID"].tolist())}
                        count = 0
                        for _, row in valid_to_entry.iterrows():
                            tid = row["Ticket_ID"]
                            if tid in id_to_index:
                                idx = id_to_index[tid]
                                tickets.at[idx, "Visited"] = True
                                tickets.at[idx, "Visitor_Seats"] = int(row["Visitor_Count"])
                                tickets.at[idx, "Timestamp"] = now_ts()
                                count += 1
                        save_tickets_df(tickets)
                        st.success(f"✅ {count} Visitor records processed.")
                        st.rerun()
                else: st.error("Invalid Columns.")

        elif v_action == "Reverse Entry":
            rv_tid = st.text_input("Enter Ticket ID to reverse entry")
            if st.button("Reverse Entry"):
                idx_list = tickets.index[tickets["TicketID"] == rv_tid.zfill(4)].tolist()
                if idx_list:
                    idx = idx_list[0]
                    tickets.at[idx, "Visited"] = False
                    tickets.at[idx, "Visitor_Seats"] = 0
                    save_tickets_df(tickets)
                    st.success("✅ Entry reversed.")
                    st.rerun()

    with v_out:
        st.write("**Recent Visitors**")
        # Removed .head(10) and added height for full record viewing via scroll
        st.dataframe(tickets[tickets["Visited"]].sort_values("Timestamp", ascending=False), 
                     hide_index=True, use_container_width=True, height=500)

# --- 4. EDIT MENU ---
with tabs[3]:
    st.subheader("Menu & Series Configuration")
    menu_display = custom_sort(menu.copy())
    edited_menu = st.data_editor(menu_display, hide_index=True, use_container_width=True, num_rows="dynamic")

    menu_pass_input = st.text_input("Enter Menu Update Password", type="password")
    if st.button("Update Database Menu"):
        if menu_pass_input == MENU_UPDATE_PASSWORD:
            new_tickets_list = []
            existing_map = {row["TicketID"]: row.to_dict() for _, row in tickets.iterrows()}
            for _, m_row in edited_menu.iterrows():
                series = str(m_row.get("Series", "")).strip()
                if "-" in series:
                    try:
                        start, end = map(int, series.split("-"))
                        for tid in range(start, end + 1):
                            tid_str = str(tid).zfill(4)
                            if tid_str in existing_map:
                                new_tickets_list.append(existing_map[tid_str])
                            else:
                                new_tickets_list.append({
                                    "TicketID": tid_str, "Category": m_row.get("Category"),
                                    "Type": m_row.get("Type"), "Admit": int(m_row.get("Admit", 1)),
                                    "Seq": m_row.get("Seq"), "Sold": False, "Visited": False,
                                    "Customer": "", "Visitor_Seats": 0, "Timestamp": None
                                })
                    except: continue
            save_both(pd.DataFrame(new_tickets_list), edited_menu)
            st.success("✅ Menu Updated.")
            st.rerun()
        else: st.error("❌ Incorrect Password")
