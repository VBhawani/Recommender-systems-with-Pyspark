from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import explode
from pyspark.ml.recommendation import ALSModel

spark = SparkSession.builder.appName("Movie Recommendation App").getOrCreate()
sc = spark.sparkContext
movies_df = spark.read.csv("../data/movies.csv",header = True)
ratings_df = spark.read.csv("../data/ratings.csv",header= True)

#convert string to integer and double
ratings_df = ratings_df.select(ratings_df.userId.cast("integer"), ratings_df.movieId.cast("integer"), 
                               ratings_df.rating.cast("double"))

def create_model():
    
    (train, test) = ratings_df.randomSplit([0.8,0.2], seed=42)

    als = ALS(regParam= 0.1,maxIter=20, userCol="userId", itemCol="movieId", ratingCol="rating", nonnegative= True,coldStartStrategy="drop")

    model = als.fit(train)
    model.save(sc,"../model")
    prediction = model.transform(test)
    
    # Calculate Root Mean Squared Error 
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(prediction)

    print(f"Root Mean Squared Error (RMSE): {rmse}")
    
    return model

def movie_reco(user_id,model):

    user_ratings = ratings_df.filter(ratings_df.userId == user_id)
    if user_ratings.count() == 0:
        # If no ratings exist for the user, recommend popular movies
        print(f"No ratings found for User {user_id}. Recommending popular movies.")
        # select top-rated movies (top 10 movies based on average ratings)
        popular_movies = ratings_df.groupBy("movieId").avg("rating").orderBy("avg(rating)", ascending=False).limit(10)
        recommended_movies = popular_movies.join(movies_df, on="movieId", how="left_outer")

        return recommended_movies.select("title")
    else:
        # If user has rated movies, recommend based on the model
        recommendations = model.recommendForUserSubset(user_ratings, 10)
        recommended_movie_ids = recommendations.select(explode("recommendations.movieId").alias("movieId"))
        recommended_movies = recommended_movie_ids.join(movies_df, on="movieId", how="left_outer")

        return recommended_movies.select("title")

def load_model(model_path):
    return ALSModel.load(model_path)
#create_model()
# movie_reco(1,model)

# model = load_model("./model")
# movie_reco(2, model)
