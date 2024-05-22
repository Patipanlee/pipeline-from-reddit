select author,count(author),max(date)
from reddit
group by author