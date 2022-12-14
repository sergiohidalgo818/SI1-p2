Select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title, STRING_AGG(mg.genre, ', ') as genres,  mo.year, x.directors
from imdb_moviegenres mg join imdb_movies mo on mg.movieid = mo.movieid 
join (
	select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title, STRING_AGG(md.directorname, ', ') as directors
	from imdb_directormovies dm join imdb_movies mo on mo.movieid = dm.movieid join imdb_directors md on dm.directorid = md.directorid
	where mo.movieid  in (
    	Select mo.movieid from imdb_movies mo 
    	join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    	order by year desc limit 400) 
	group by mo.year, mo.movieid 
	order by mo.year desc) as x
on title = x.title
where mg.movieid  in (
    Select mo.movieid from imdb_movies mo 
    join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    order by year desc limit 400) 
group by title, mo.year , mo.movietitle, x.directors
order by mo.year desc, title desc; 

