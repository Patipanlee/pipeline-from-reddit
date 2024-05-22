select date, count(DISTINCT title), avg(score)
from reddit
group by date