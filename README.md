# HadoopPractices
MapReduce programs on Hadoop<br/>
All dataset in src folder should be uploaded to DFS first.<br/>
All programs are in test folder.<br/>
<br/>
# 1. Inverted Index
Extract words from each file, and output each word with its sources.
<br/>
# 2. TopK Movies
Dataset from [MovieLens Latest Datasets](http://files.grouplens.org/datasets/movielens/ml-latest-small.zip)<br/>
Only used `ratings.csv` for test.<br/>
More explanation is in my blog:[TopK movies on Hadoop](http://www.blog.han-yu.me/blog/tiHXGExFPv2JAXJ8W)<br/>
Test with an extra arg `K`<br/>
<br/>
Top 15 movies are as follows:
1. 46	4.948717948717949
2. 443	4.85
3. 298	4.8
4. 448	4.75
5. 622	4.725806451612903
6. 113	4.62962962962963
7. 446	4.62
8. 89	4.590909090909091
9. 287	4.531496062992126
10. 656	4.5234375
11. 40	4.511627906976744
12. 454	4.5
13. 145	4.5
14. 230	4.473404255319149
15. 544	4.472014925373134
