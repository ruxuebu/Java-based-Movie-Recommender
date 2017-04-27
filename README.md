# Movie-Recommendation-System-Hadoop
Item-Item collaborative filtering algorithms is not based on the item(movie,book) content(genre, author, keywords…) Rather, it is based on the User’s past history like buying, viewing, rating items.

Item-based collaborative filtering is a model-based algorithm for making recommendations. In the algorithm, the similarities between different items in the dataset are calculated by using one of a number of similarity measures, and then these similarity values are used to predict ratings for user-item pairs not present in the dataset.

1. Reorganized the data(group the data by userId)

   Map method -> Input: user, movie_id, rating
   
   Reduce method -> Output: key=userId, value=<movieId:rating, movieId:rating..>

2. Generated the Cooccurrence matrix(get the relation between movie1 and movie2):

   Map method -> Input: userId \t movie1:rating1, movie2:rating…
              -> Output: key=movie1:movie2, value = 1…
              
   Reduce method -> Input: key=movie1:movie2, value = iterable<1,1,1>

3. predict the score:
   Map method -> setup and get the cooccurrence matrix 
                 Input: movie1:movie2 \t relation
                 Input: userId, movie, rating
                 Output: userId: movie score
                 
   Reduce method: -> Output: userId:movie \t score

4. Generate the recommendation list:

   Map method: Read movie watch history-> Input: userId, movie_Id, rating
               filter out the movies that the user has watched before-> Input: user   \t movie:rating
               
   Reduce method: read<movie_Id, movie_name>, remove movie name
                  match movie_name to movie_id-> Input: user \t movie_id:rating
                  -> Output: movie_name

5. Generate the top k-score movie recommendation list:

   Output: user[i]: movie
