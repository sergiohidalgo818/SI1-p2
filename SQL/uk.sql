--Query1: consulta para obtener titulo, generos, año, numero de votos, calificacion, directores y actores
Select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title, z.genres, 
mo.year, mo.ratingcount, mo.ratingmean, x.directors, y.actors
from imdb_movies mo
join (
	select movieid , STRING_AGG(concat('\"' , md.directorname , '\"'), ', ') as directors
	from imdb_directormovies dm join imdb_directors md on dm.directorid = md.directorid
	where movieid in (
    Select mo.movieid from imdb_movies mo 
    join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    order by year desc)
    group by dm.movieid) as x
on mo.movieid = x.movieid
join(
    select movieid, STRING_AGG(concat('\"', ma.actorname , '\"'), ', ') as actors
	from imdb_actormovies am join imdb_actors ma on am.actorid = ma.actorid
	where am.movieid in (
    Select mo.movieid from imdb_movies mo 
    join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    order by year desc )
    group by am.movieid) as y 
on mo.movieid = y.movieid
join (
	select movieid, STRING_AGG(concat('\"',mg.genre, '\"'), ', ') as genres
	from imdb_moviegenres mg 
	where mg.movieid in (
    Select mo.movieid from imdb_movies mo 
    join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    order by year desc)
    group by movieid) as z
on mo.movieid = z.movieid
where mo.movieid in (
    Select mo.movieid from imdb_movies mo 
    join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    order by year desc) 
group by title, mo.year , mo.movietitle, x.directors, mo.ratingcount, mo.ratingmean, y.actors, z.genres
order by mo.year desc, title desc limit 400; 


---Query2: Consulta que obtiene todos los títulos años, fechas y generos   
Select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title, 
mo.year, mo.ratingcount, STRING_AGG(mg.genre, ', ') as genres
from imdb_moviegenres mg join imdb_movies mo on mg.movieid = mo.movieid 
where replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') != %s  and mg.movieid in (
    Select mo.movieid from imdb_movies mo 
    join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    order by year desc limit 400) 
group by title, mo.year, mo.ratingcount
order by mo.year desc