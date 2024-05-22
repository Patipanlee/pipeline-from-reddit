select count(DISTINCT author) as total_author,count(DISTINCT title) as total_post,avg(score) as total_score
from reddit
