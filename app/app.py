
import streamlit as st
import pandas as pd
from main import load_model, movie_reco  

# Loading pre-trained ALS model
model = load_model("./model")

st.title("Movie Recommendation System")
st.write("""
    Welcome to the Movie Recommendation App! \n
    Enter your user ID to receive personalized movie recommendations.
""")

#taking input from user of userId
user_id = st.number_input("Enter User ID", step=1, value=1)

if st.button("Get Recommendations"):
    st.write(f"Fetching movie recommendations for User ID: {user_id}...")

    recommendations = movie_reco(user_id, model)
    """it checks whether model has returned any recommendations, if yes it will take first 10 """
    if recommendations.count() > 0:
        recommendations_list = recommendations.take(10)

        """convert each row from Spark DataFrame into dictionary,"""
        recommendations_data = []
        for row in recommendations_list:
            recommendations_data.append(row.asDict())

        """convert dict into pandas dataframe"""
        df_recommendations = pd.DataFrame(recommendations_data)

        # Display table in Streamlit
        st.dataframe(df_recommendations)
    else:
        st.write("No recommendations available for this user.")
    """If there are no recommendation if the user has not rated any movies or the model could 
    not generate recommendations, it displays the message."""