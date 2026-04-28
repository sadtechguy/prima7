import streamlit as st
import streamlit_authenticator as stauth

def check_login():
    """
    Handles user login. 
    Halts the app if not logged in.
    Returns the authenticator object if successful (so we can use the logout button).
    """
    
    # 1. Fetch the credentials and cookie settings from secrets.toml
    # Using .get() is safer in case a key is accidentally missing

    # --- AUTHENTICATION SETUP ---
    # Fetch the credentials and cookie settings from secrets.toml
    credentials = st.secrets["credentials"].to_dict()
    cookie_name = st.secrets["cookie"]["name"]
    cookie_key = st.secrets["cookie"]["key"]
    cookie_expiry = st.secrets["cookie"]["expiry_days"]

    # 2. Initialize the authenticator
    authenticator = stauth.Authenticate(
        credentials,
        cookie_name,
        cookie_key,
        cookie_expiry
    )

    # 3. Display the login widget on the main screen
    # Some versions of stauth return values here, but they always update session_state
    authenticator.login()

    # 4. --- THE SECURITY GATE ---
    # We use .get() so Streamlit doesn't crash on the very first load
    auth_status = st.session_state.get("authentication_status")

    if auth_status is False:
        st.error('❌ Username/password is incorrect')
        st.stop() # 🛑 STOPS THE APP HERE
    elif auth_status is None:
        st.warning('🔒 Please enter your username and password to access AreaMapper')
        st.stop() # 🛑 STOPS THE APP HERE
    # 5. If they made it past the stops, they are logged in!
    # We return the authenticator so app.py can place the logout button
    return authenticator