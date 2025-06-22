import streamlit as st
import pandas as pd
import mysql.connector
from datetime import date 
from streamlit_option_menu import option_menu # Make sure you have this installed: pip install streamlit-option-menu


# --- MySQL Database Connection Details ---
# IMPORTANT: Replace these with your actual MySQL server details
DB_HOST = "localhost"
DB_USER = "vikram"  # e.g., "root"
DB_PASSWORD = "Vikram" # Your MySQL user's password
DB_NAME = "project_1" # The name of your database

# --- SQL Queries ---
# This dictionary holds all your queries, organized by a descriptive title.
QUERIES = {
    "0. All Filtered Asteroid Details": """
        SELECT
            a.id,
            a.name,
            a.absolute_magnitude_h,
            a.estimated_diameter_min_km,
            a.estimated_diameter_max_km,
            a.is_potentially_hazardous_asteroid,
            ca.close_approach_date,
            ca.relative_velocity_kmph,
            ca.astronomical,
            ca.miss_distance_km,
            ca.miss_distance_lunar,
            ca.orbiting_body
        FROM
            asteroids AS a
        JOIN
            close_approach AS ca ON a.id = ca.neo_reference_id
    """,
    "1. Count how many times each asteroid has approached Earth": """
        SELECT
            a.name AS asteroid_name,
            COUNT(a.id) AS number_of_approaches
        FROM
            asteroids AS a
        LEFT JOIN
            close_approach AS ca ON a.id = ca.neo_reference_id
        GROUP BY
            a.name
        ORDER BY
            number_of_approaches DESC, a.name;
    """,
    "2. Average velocity of each asteroid over multiple approaches": """
        SELECT
            a.name AS asteroid_name,
            AVG(ca.relative_velocity_kmph) AS average_velocity_kmph
        FROM
            asteroids AS a
        JOIN
            close_approach AS ca ON a.id = ca.neo_reference_id
        GROUP BY
            a.name
        HAVING
            COUNT(a.id) > 1
        ORDER BY
            average_velocity_kmph DESC;
    """,
    "3. List top 10 fastest asteroids (based on any approach)": """
        SELECT
            a.name AS asteroid_name,
            MAX(ca.relative_velocity_kmph) AS fastest_velocity_kmph,
            ca.close_approach_date
        FROM
            asteroids AS a
        JOIN
            close_approach AS ca ON a.id = ca.neo_reference_id
        GROUP BY
            a.name, ca.close_approach_date
        ORDER BY
            fastest_velocity_kmph DESC
        LIMIT 10;
    """,
    "4. Find potentially hazardous asteroids that have approached Earth more than 3 times": """
        SELECT
            a.name AS asteroid_name,
            COUNT(a.id) AS number_of_approaches
        FROM
            asteroids AS a
        JOIN
            close_approach AS ca ON a.id = ca.neo_reference_id
        WHERE
            a.is_potentially_hazardous_asteroid = TRUE
        GROUP BY
            a.name
        HAVING
            COUNT(a.id) > 3
        ORDER BY
            number_of_approaches DESC;
    """,
    "5. Find the month with the most asteroid approaches": """
        SELECT
            DATE_FORMAT(close_approach_date, '%Y-%m') AS approach_month,
            COUNT(neo_reference_id) AS approaches_count
        FROM
            close_approach
        GROUP BY
            approach_month
        ORDER BY
            approaches_count DESC
        LIMIT 1;
    """,
    "6. Get the asteroid with the fastest ever approach speed": """
        SELECT
            a.name AS asteroid_name,
            ca.relative_velocity_kmph,
            ca.close_approach_date
        FROM
            asteroids AS a
        JOIN
            close_approach AS ca ON a.id = ca.neo_reference_id
        ORDER BY
            ca.relative_velocity_kmph DESC
        LIMIT 1;
    """,
    "7. Sort asteroids by maximum estimated diameter (descending)": """
        SELECT
            name AS asteroid_name,
            estimated_diameter_max_km
        FROM
            asteroids
        ORDER BY
            estimated_diameter_max_km DESC;
    """,
    "8. An asteroid whose closest approach is getting nearer over time (decreasing astronomical distance for later dates)": """
        SELECT DISTINCT
            a.name AS asteroid_name
        FROM
            asteroids AS a
        JOIN
            close_approach AS ca1 ON a.id = ca1.neo_reference_id
        JOIN
            close_approach AS ca2 ON a.id = ca2.neo_reference_id
        WHERE
            ca1.close_approach_date < ca2.close_approach_date
            AND ca1.astronomical > ca2.astronomical -- Using 'astronomical' for AU distance
        GROUP BY
            a.name
        HAVING
            COUNT(DISTINCT ca1.neo_reference_id) > 1;
    """,
    "9. Display the name of each asteroid along with the date and miss distance of its closest approach to Earth": """
        SELECT
            a.name AS asteroid_name,
            MIN(ca.astronomical) AS closest_astronomical_distance,
            SUBSTRING_INDEX(GROUP_CONCAT(ca.close_approach_date ORDER BY ca.astronomical ASC), ',', 1) AS closest_approach_date
        FROM
            asteroids AS a
        JOIN
            close_approach AS ca ON a.id = ca.neo_reference_id
        GROUP BY
            a.name
        ORDER BY
            closest_astronomical_distance ASC;
    """,
    "10. List names of asteroids that approached Earth with velocity > 50,000 km/h": """
        SELECT DISTINCT
            a.name AS asteroid_name,
            ca.relative_velocity_kmph,
            ca.close_approach_date
        FROM
            asteroids AS a
        JOIN
            close_approach AS ca ON a.id = ca.neo_reference_id
        WHERE
            ca.relative_velocity_kmph > 50000
        ORDER BY
            ca.relative_velocity_kmph DESC;
    """,
    "11. Count how many approaches happened per month": """
        SELECT
            DATE_FORMAT(close_approach_date, '%Y-%m') AS approach_month,
            COUNT(neo_reference_id) AS approaches_count
        FROM
            close_approach
        GROUP BY
            approach_month
        ORDER BY
            approach_month;
    """,
    "12. Find asteroid with the highest brightness (lowest magnitude value)": """
        SELECT
            name AS asteroid_name,
            absolute_magnitude_h
        FROM
            asteroids
        ORDER BY
            absolute_magnitude_h ASC
        LIMIT 1;
    """,
    "13. Get number of hazardous vs non-hazardous asteroids": """
        SELECT
            CASE
                WHEN is_potentially_hazardous_asteroid = TRUE THEN 'Hazardous'
                ELSE 'Non-Hazardous'
            END AS hazard_status,
            COUNT(id) AS asteroid_count
        FROM
            asteroids
        GROUP BY
            hazard_status;
    """,
    "14. Find asteroids that passed closer than the Moon (lesser than 1 LD), along with their close approach date and distance": """
        SELECT
            a.name AS asteroid_name,
            ca.close_approach_date,
            ca.miss_distance_lunar AS miss_distance_lunar_distances,
            ca.astronomical AS astronomical_units_distance
        FROM
            asteroids AS a
        JOIN
            close_approach AS ca ON a.id = ca.neo_reference_id
        WHERE
            ca.miss_distance_lunar < 1
        ORDER BY
            ca.miss_distance_lunar ASC;
    """,
    "15. Find asteroids that came within 0.05 AU (astronomical distance)": """
        SELECT DISTINCT
            a.name AS asteroid_name,
            ca.close_approach_date,
            ca.astronomical
        FROM
            asteroids AS a
        JOIN
            close_approach AS ca ON a.id = ca.neo_reference_id
        WHERE
            ca.astronomical <= 0.05
        ORDER BY
            ca.astronomical ASC;
    """,
    "16. Find asteroids with specific orbit characteristics (e.g., a specific orbit ID pattern)": """
        SELECT
            name AS asteroid_name,
            is_potentially_hazardous_asteroid
        FROM
            asteroids
        WHERE
            name LIKE '6%'; -- Example: Finds asteroids where name starts with '6'
    """,
    "17. Calculate the total number of unique asteroids observed in approaches within a specific year (e.g., 2024)": """
        SELECT
            COUNT(DISTINCT a.id) AS unique_asteroids_in_year
        FROM
            asteroids AS a
        JOIN
            close_approach AS ca ON a.id = ca.neo_reference_id
        WHERE
            YEAR(ca.close_approach_date) = 2024;
    """,
    "18. List asteroids that are NOT potentially hazardous but have a very close approach distance (e.g., less than 0.001 AU)": """
        SELECT DISTINCT
            a.name AS asteroid_name,
            ca.close_approach_date,
            ca.astronomical AS astronomical_distance,
            a.is_potentially_hazardous_asteroid
        FROM
            asteroids AS a
        JOIN
            close_approach AS ca ON a.id = ca.neo_reference_id
        WHERE
            a.is_potentially_hazardous_asteroid = FALSE
            AND ca.astronomical < 0.001
        ORDER BY
            ca.astronomical ASC;
    """,
    "19. For each asteroid, find its earliest and latest recorded close approach dates": """
        SELECT
            a.name AS asteroid_name,
            MIN(ca.close_approach_date) AS earliest_approach_date,
            MAX(ca.close_approach_date) AS latest_approach_date,
            COUNT(ca.neo_reference_id) AS total_approaches
        FROM
            asteroids AS a
        JOIN
            close_approach AS ca ON a.id = ca.neo_reference_id
        GROUP BY
            a.name
        ORDER BY
            a.name;
    """,
    "20. Count the number of approaches grouped by velocity ranges (e.g., <20k, 20k-50k, >50k km/h)": """
        SELECT
            CASE
                WHEN relative_velocity_kmph < 20000 THEN 'Slow (< 20,000 km/h)'
                WHEN relative_velocity_kmph >= 20000 AND relative_velocity_kmph <= 50000 THEN 'Medium (20,000-50,000 km/h)'
                ELSE 'Fast (> 50,000 km/h)'
            END AS velocity_range,
            COUNT(neo_reference_id) AS number_of_approaches
        FROM
            close_approach
        GROUP BY
            velocity_range
        ORDER BY
            MIN(relative_velocity_kmph);
    """
}

# --- Streamlit App Layout ---

st.set_page_config(layout="wide", page_title="Asteroid Data Analysis")

st.title("🛰️ Asteroid Close Approach Analysis")
st.markdown("Explore various insights from hypothetical asteroid close approach data using SQL queries.")


# --- Database Connection Function ---
@st.cache_resource # Cache the connection to avoid re-establishing on every rerun
def get_db_connection():
    """Establishes a connection to the MySQL database."""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        # Set group_concat_max_len for query 9 specifically if needed
        cursor = conn.cursor()
        cursor.execute("SET SESSION group_concat_max_len = 100000;")
        cursor.close()
        return conn
    except mysql.connector.Error as err:
        st.error(f"Error connecting to MySQL database: {err}")
        st.stop() # Stop the app if connection fails
        return None

# Get the database connection
conn = get_db_connection()

if conn: # Proceed only if the database connection is successful
    st.success("Successfully connected to the MySQL database!")

    # --- Session State Initialization ---
    # Streamlit's session state allows preserving variable values across reruns.
    # This is crucial for remembering filter selections.
    if 'asteroid_name_filter' not in st.session_state:
        st.session_state.asteroid_name_filter = ""
    if 'is_hazardous_filter' not in st.session_state:
        st.session_state.is_hazardous_filter = "All"
    if 'velocity_range_filter' not in st.session_state:
        st.session_state.velocity_range_filter = (0.0, 200000.0)
    if 'date_range_filter' not in st.session_state:
        st.session_state.date_range_filter = (date(2000, 1, 1), date.today())
    if 'magnitude_range_filter' not in st.session_state:
        st.session_state.magnitude_range_filter = (0.0, 40.0)
    if 'diameter_range_filter' not in st.session_state:
        st.session_state.diameter_range_filter = (0.0, 100.0)
    if 'astronomical_range_filter' not in st.session_state:
        st.session_state.astronomical_range_filter = (0.0, 1.0)
    if 'selected_orbiting_bodies' not in st.session_state:
        st.session_state.selected_orbiting_bodies = []
    if 'selected_query_title' not in st.session_state:
        st.session_state.selected_query_title = list(QUERIES.keys())[0]

    # --- Helper Function: Build Dynamic WHERE Clause ---
    # This function is the core of dynamic filtering. It intelligently constructs
    # the SQL WHERE clause based on current filter selections AND the structure
    # of the base SQL query being modified.
    def build_dynamic_where_clause_from_session_state(base_sql_query_to_analyze):
        """
        Builds a dynamic WHERE clause based on session state filters.
        It adapts column prefixes (e.g., 'a.name' vs 'name') based on
        whether aliases are used in the provided base SQL query.
        """
        conditions = [] # List to hold individual WHERE conditions

        # Convert the base query to uppercase for case-insensitive keyword checking
        upper_query_for_analysis = base_sql_query_to_analyze.upper()

        # Determine if 'asteroids' or 'close_approach' tables are present in the base query.
        # This helps avoid adding filters for tables not involved in the current query.
        has_asteroids_table = 'FROM ASTEROIDS' in upper_query_for_analysis or 'JOIN ASTEROIDS' in upper_query_for_analysis
        has_close_approach_table = 'FROM CLOSE_APPROACH' in upper_query_for_analysis or 'JOIN CLOSE_APPROACH' in upper_query_for_analysis

        # Check if specific aliases 'AS A' or 'AS CA' are used.
        # This dictates whether we prepend 'a.' or 'ca.' to column names.
        uses_a_alias = 'FROM ASTEROIDS AS A' in upper_query_for_analysis or 'JOIN ASTEROIDS AS A' in upper_query_for_analysis
        uses_ca_alias = 'FROM CLOSE_APPROACH AS CA' in upper_query_for_analysis or 'JOIN CLOSE_APPROACH AS CA' in upper_query_for_analysis

        # --- Apply Filters Conditionally based on Table Presence and Alias Usage ---

        # Asteroid Name filter (applies to 'asteroids' table)
        if st.session_state.asteroid_name_filter and has_asteroids_table:
            # Use 'a.' prefix if 'asteroids AS a' alias is present, otherwise no prefix.
            prefix = 'a.' if uses_a_alias else ''
            conditions.append(f"{prefix}name LIKE '%{st.session_state.asteroid_name_filter}%'")

        # Hazardous filter (applies to 'asteroids' table)
        if st.session_state.is_hazardous_filter != "All" and has_asteroids_table:
            hazardous_value = "TRUE" if st.session_state.is_hazardous_filter == "Yes" else "FALSE"
            prefix = 'a.' if uses_a_alias else ''
            conditions.append(f"{prefix}is_potentially_hazardous_asteroid = {hazardous_value}")

        # Velocity range filter (applies to 'close_approach' table)
        min_vel, max_vel = st.session_state.velocity_range_filter
        if (min_vel > 0.0 or max_vel < 200000.0) and has_close_approach_table:
            prefix = 'ca.' if uses_ca_alias else ''
            conditions.append(f"{prefix}relative_velocity_kmph BETWEEN {min_vel} AND {max_vel}")

        # Close Approach Date Range filter (applies to 'close_approach' table)
        start_date, end_date = st.session_state.date_range_filter
        # Ensure date range is valid before applying
        if start_date and end_date and start_date <= end_date and has_close_approach_table:
            prefix = 'ca.' if uses_ca_alias else ''
            conditions.append(f"{prefix}close_approach_date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'")

        # Absolute Magnitude Range filter (applies to 'asteroids' table)
        min_mag, max_mag = st.session_state.magnitude_range_filter
        if (min_mag > 0.0 or max_mag < 40.0) and has_asteroids_table:
            prefix = 'a.' if uses_a_alias else ''
            conditions.append(f"{prefix}absolute_magnitude_h BETWEEN {min_mag} AND {max_mag}")

        # Estimated Diameter Range filter (applies to 'asteroids' table)
        min_diam, max_diam = st.session_state.diameter_range_filter
        if (min_diam > 0.0 or max_diam < 100.0) and has_asteroids_table:
            prefix = 'a.' if uses_a_alias else ''
            conditions.append(f"{prefix}estimated_diameter_min_km BETWEEN {min_diam} AND {max_diam}")

        # Astronomical Unit Distance Range filter (applies to 'close_approach' table)
        min_au, max_au = st.session_state.astronomical_range_filter
        if (min_au > 0.0 or max_au < 1.0) and has_close_approach_table:
            prefix = 'ca.' if uses_ca_alias else ''
            conditions.append(f"{prefix}astronomical BETWEEN {min_au} AND {max_au}")

        # Orbiting Body filter (applies to 'close_approach' table)
        if st.session_state.selected_orbiting_bodies and has_close_approach_table:
            # Format the list of selected bodies into a comma-separated string for SQL IN clause
            quoted_bodies = [f"'{body}'" for body in st.session_state.selected_orbiting_bodies]
            prefix = 'ca.' if uses_ca_alias else ''
            conditions.append(f"{prefix}orbiting_body IN ({', '.join(quoted_bodies)})")

        # MODIFIED: Join conditions with newline + AND for multi-line display and indentation
        if conditions:
            # The first condition is preceded by "WHERE ". Subsequent conditions start with "\n  AND "
            return "WHERE " + "\n  AND ".join(conditions)
        else:
            return "" # Return empty string if no filters are applied

    # --- Sidebar Navigation ---
    # Uses `streamlit_option_menu` for a cleaner sidebar navigation.
    with st.sidebar:
        st.header("Navigation")
        selected_sidebar_option = option_menu(
            menu_title=None, # No main title for the menu
            options=["Filter Criteria", "Queries"], # Options to display
            icons=["funnel", "search"], # Icons for each option
        )
        st.markdown("---") # Visual separator
        st.info("Data is hypothetical for demonstration purposes.")

    # --- Main Content Area: Conditional Rendering based on Sidebar Selection ---

    if selected_sidebar_option == "Filter Criteria":
        st.subheader("Apply Data Filters")
        st.markdown("Use the sliders and selectors below to refine the data for your queries.")

        # Layout filters in two columns for better organization
        col1, col2 = st.columns(2)

        with col1:
            st.session_state.asteroid_name_filter = st.text_input(
                "Filter by Asteroid Name (partial match)",
                value=st.session_state.asteroid_name_filter,
                key="name_filter_input" # Unique key for Streamlit widgets
            )
            st.session_state.magnitude_range_filter = st.slider(
                "Absolute Magnitude (H) Range",
                min_value=0.0, max_value=40.0,
                value=st.session_state.magnitude_range_filter,
                step=0.1,
                key="magnitude_filter_slider"
            )
            st.session_state.diameter_range_filter = st.slider(
                "Estimated Diameter (km) Range",
                min_value=0.0, max_value=100.0,
                value=st.session_state.diameter_range_filter,
                step=0.1,
                key="diameter_filter_slider"
            )

        with col2:
            st.session_state.velocity_range_filter = st.slider(
                "Relative Velocity (km/h) Range",
                min_value=0.0, max_value=200000.0,
                value=st.session_state.velocity_range_filter,
                step=1000.0,
                key="velocity_filter_slider"
            )
            # Ensure date range value is a valid tuple for st.date_input
            current_date_range_value = st.session_state.date_range_filter
            if not isinstance(current_date_range_value, tuple) or len(current_date_range_value) != 2:
                current_date_range_value = (date(2000, 1, 1), date.today())

            st.session_state.date_range_filter = st.date_input(
                "Close Approach Date Range",
                value=current_date_range_value,
                min_value=date(1900, 1, 1),
                max_value=date(2100, 1, 1),
                key="date_range_filter_input"
            )
            st.session_state.astronomical_range_filter = st.slider(
                "Astronomical Unit (AU) Distance Range",
                min_value=0.0, max_value=1.0,
                value=st.session_state.astronomical_range_filter,
                step=0.001,
                key="astronomical_filter_slider"
            )
            st.session_state.is_hazardous_filter = st.selectbox(
                "Potentially Hazardous Asteroid?",
                options=["All", "Yes", "No"],
                index=["All", "Yes", "No"].index(st.session_state.is_hazardous_filter),
                key="hazardous_filter_selectbox"
            )

            # Function to fetch orbiting bodies, cached for performance
            @st.cache_data(ttl=3600)
            def get_orbiting_bodies(_connection):
                try:
                    df_bodies = pd.read_sql_query("SELECT DISTINCT orbiting_body FROM close_approach WHERE orbiting_body IS NOT NULL ORDER BY orbiting_body;", _connection)
                    return df_bodies['orbiting_body'].tolist()
                except Exception as e:
                    st.error(f"Error fetching orbiting bodies: {e}")
                    return []

            unique_orbiting_bodies = get_orbiting_bodies(conn)
            st.session_state.selected_orbiting_bodies = st.multiselect(
                "Filter by Orbiting Body",
                options=unique_orbiting_bodies,
                default=st.session_state.selected_orbiting_bodies,
                key="orbiting_body_filter_multiselect"
            )

        st.markdown("---") # Visual separator
        st.subheader("Filter Summary")
        st.write("Number of Unique Asteroids Matching Filters:")

        # --- Generic Base Query for Filter Summary Count ---
        # This query is designed to count unique asteroids after applying ALL filters.
        # It explicitly joins both tables ('asteroids' and 'close_approach')
        # to ensure all filter types (asteroid properties AND close approach properties)
        # can be considered. The aliases 'a' and 'ca' are used here.
        base_count_query = """
            SELECT COUNT(DISTINCT a.id)
            FROM asteroids AS a
            JOIN close_approach AS ca ON a.id = ca.neo_reference_id
        """

        # Build the dynamic WHERE clause using the current filter selections.
        # The `build_dynamic_where_clause_from_session_state` function
        # will correctly use 'a.' and 'ca.' prefixes because they are present
        # in `base_count_query`.
        dynamic_where_clause_for_count = build_dynamic_where_clause_from_session_state(base_count_query)

        # Combine the base count query with the generated WHERE clause
        final_count_query_for_summary = base_count_query + " " + dynamic_where_clause_for_count

        # Optional: Display the SQL query used for the count (useful for debugging)
        # st.code(final_count_query_for_summary, language="sql", title="SQL Query for Filter Summary")

        try:
            # Execute the count query using a database cursor
            cursor = conn.cursor()
            cursor.execute(final_count_query_for_summary)
            count_result = cursor.fetchone()[0] # Fetch the single count value
            cursor.close()

            # Display the count using st.metric for a prominent display
            st.metric(label="Unique Asteroids Found", value=f"{count_result:,}")
            st.info("This count reflects the number of unique asteroids that satisfy ALL currently applied filters. It updates automatically as you change filters.")

            #Filter use
            if st.button("View Matching Asteroids in Detail"):
                st.session_state.selected_sidebar_optiion = "Queries"
                st.session_state.selected_query_title =  "0. All Filtered Asteroid Details"  
                st.rerun()

        except mysql.connector.Error as e:
            st.error(f"Error retrieving filter summary: {e}")
            st.info("Please ensure your database is running and contains data compatible with the filters. Some filter combinations might not apply to this summary count (e.g., if a filter requires a table not present in the generic count query).")
        except Exception as e:
            st.error(f"An unexpected error occurred during filter summary: {e}")


    elif selected_sidebar_option == "Queries":
        st.subheader("Run Asteroid Queries")
        st.markdown("Select a query from the dropdown to see insights. Filters (if applied on the 'Filter Criteria' page) will affect the results.")

        # Dropdown to select a predefined query
        selected_query_title = st.selectbox(
            "Choose an analysis:",
            list(QUERIES.keys()),
            index=list(QUERIES.keys()).index(st.session_state.selected_query_title),
            key="query_selector_main"
        )
        st.session_state.selected_query_title = selected_query_title # Update session state

        st.markdown("---") # Visual separator

        base_sql_query = QUERIES.get(selected_query_title, "").strip()

        # Build the dynamic WHERE clause, adapting to the selected query's structure
        dynamic_where_clause_text = build_dynamic_where_clause_from_session_state(base_sql_query)

        final_sql_query = base_sql_query # Start with the base query

        # --- Logic to Inject WHERE Clause into the Base Query ---
        # This section ensures the dynamic WHERE clause is inserted correctly
        # without breaking existing GROUP BY, ORDER BY, or LIMIT clauses.
        if dynamic_where_clause_text: # Only proceed if there are filters to apply
            upper_base_query = base_sql_query.upper()

            # Keywords that typically define structural parts of a SQL query
            keywords_for_splitting = ['GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT']

            insertion_point = len(base_sql_query) # Default: append to the end

            # Find the earliest occurrence of any structural keyword
            # This is where the WHERE clause should be inserted before.
            for keyword in keywords_for_splitting:
                idx = upper_base_query.find(keyword)
                # Ensure it's a whole word match or at the start of the string
                if idx != -1 and (idx == 0 or not upper_base_query[idx-1].isalpha()):
                    insertion_point = min(insertion_point, idx)

            # Split the query into the part before keywords and the part after
            part_before_keywords = base_sql_query[:insertion_point].strip()
            part_after_keywords = base_sql_query[insertion_point:].strip()

            # Check if the part before keywords already contains a WHERE clause
            has_existing_where = "WHERE" in part_before_keywords.upper()

            if has_existing_where:
                # If an existing WHERE clause is found, append new conditions using " AND "
                existing_where_pos = part_before_keywords.upper().find("WHERE")
                # Extract existing conditions (everything after the first WHERE)
                existing_conditions_part = part_before_keywords[existing_where_pos + len("WHERE"):].strip()

                # Get only the conditions from the dynamically generated clause (remove "WHERE ")
                # This `dynamic_conditions_formatted` string now contains newlines and indentation
                dynamic_conditions_formatted = dynamic_where_clause_text[len("WHERE "):].strip()

                if existing_conditions_part:
                    # Combine existing conditions with dynamically generated conditions
                    # Put dynamic conditions on a new line and indent for readability
                    final_sql_query = f"{part_before_keywords[:existing_where_pos + len('WHERE')]} {existing_conditions_part}\n  AND {dynamic_conditions_formatted}"
                else:
                    # If existing WHERE had no conditions, just use the dynamic ones directly
                    final_sql_query = f"{part_before_keywords[:existing_where_pos + len('WHERE')]} {dynamic_conditions_formatted}"

                # Add the remaining part of the query on a new line if it exists
                if part_after_keywords:
                    final_sql_query += f"\n{part_after_keywords}"
            else:
                # No existing WHERE clause, so insert the dynamic WHERE clause on a new line,
                # then add the rest of the query on a new line if it exists.
                final_sql_query = f"{part_before_keywords}\n{dynamic_where_clause_text}"
                if part_after_keywords:
                    final_sql_query += f"\n{part_after_keywords}"

        # REMOVED: This line used to collapse the entire query into a single line.
        # final_sql_query = ' '.join(final_sql_query.split()).strip()

        st.write("### Generated SQL Query:")
        # st.code will now display the query with the newlines
        st.code(final_sql_query, language="sql") 

        try:
            # Execute the final SQL query and load results into a Pandas DataFrame
            df = pd.read_sql_query(final_sql_query, conn)

            st.write(f"DataFrame loaded successfully. Shape: {df.shape}")
            if df.empty:
                st.info("The DataFrame is empty. No results to display for these filters and query.")

            if not df.empty:
                st.write("### Query Results:")
                st.dataframe(df, use_container_width=True) # Display results
            else:
                st.info("No data found for this query with the applied filters. Try adjusting your filter criteria.")

        except mysql.connector.Error as e:
            st.error(f"Error executing query: {e}. Please check the generated SQL query above and try running it in your MySQL client to debug.")
            st.code(final_sql_query, language="sql") # Show the faulty query again
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
            st.code(final_sql_query, language="sql") # Show the faulty query again

else:
    # Message if database connection fails
    st.warning("Could not establish a database connection. Please select an option from the sidebar, and ensure your MySQL server is running and credentials are correct.")

st.markdown("---")
st.caption("Developed by Vikramselvaganesh.s | Powered by Streamlit & MySQL")
# To run in VS Code:
# 1. Save this code as app_vs.py (or any .py file).
# 2. Open your terminal in VS Code (Ctrl+Shift+`).
# 3. Navigate to the directory where you saved the file
# 4. Run the Streamlit app: vs coede `python -m streamlit run app_vs.py`
