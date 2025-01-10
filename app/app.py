
import streamlit as st
from main import load_model, movie_reco  

# Load the pre-trained ALS model
model = load_model("./model")

st.title("Movie Recommendation System")
st.write("""
    Welcome to the Movie Recommendation App! \n
    Enter your user ID to receive personalized movie recommendations.
""")


user_id = st.number_input("Enter User ID", step=1, value=1)

if st.button("Get Recommendations"):
    st.write(f"Fetching movie recommendations for User ID: {user_id}...")

    # Get the recommended movies for the user
    recommendations = movie_reco(user_id, model)

    # Check if recommendations are available
    if recommendations.count() > 0:
        st.write("Here are the top 10 movie recommendations:")
        recommendations_list = recommendations.take(10)  # Get the top 10 rows
        st.write(recommendations_list)  
    else:
        st.write("No recommendations available for this user.")
