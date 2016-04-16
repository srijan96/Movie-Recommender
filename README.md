# Movie-Recommender
This is a very simple Movie Recommender in Hadoop , written in java.

The whole job is broken in 4 Map-Reduce jobs which are to be run sequentially as shown in movies.sh
  
    The steps are
    <1> Normalization
    <2> Finding Distances
    <3> Contribution of Rating    and
    <4> Adding up the Ratings

    In Normalisation Phase , ratings are normalised w.r.t to the averege rating given by the user

    Next , Distance of ratings are calculated between each pair of users. 
    Here Cosine distance is used as the distance metric

    Next , each user contributes a part of his/her rating to other users based on their distance.
    The less there distance , the more contribution happens.

    Contribution phase may emit more than one rating for the same movie.
    To combine them this final Addition is used.
