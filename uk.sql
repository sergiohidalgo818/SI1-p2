--consulta para obtener titulo, genero, año, numero d votos, calificacion, directores y actores
Select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title, STRING_AGG(concat('\"',mg.genre, '\"'), ', ') as genres, 
mo.year, mo.ratingcount, mo.ratingmean, x.directors, y.actors
from imdb_moviegenres mg join imdb_movies mo on mg.movieid = mo.movieid 
join (
	select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title2, STRING_AGG(concat('\"' , md.directorname , '\"'), ', ') as directors
	from imdb_directormovies dm join imdb_movies mo on mo.movieid = dm.movieid join imdb_directors md on dm.directorid = md.directorid
	where mo.movieid in (
    Select mo.movieid from imdb_movies mo 
    join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    order by year desc limit 400)
    group by mo.year, mo.movieid 
	order by mo.year desc) as x
on replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') = x.title2
join(
    select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title2, STRING_AGG(concat('\"', ma.actorname , '\"'), ', ') as actors
	from imdb_actormovies am join imdb_movies mo on mo.movieid = am.movieid join imdb_actors ma on am.actorid = ma.actorid
	where mo.movieid in (
    Select mo.movieid from imdb_movies mo 
    join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    order by year desc limit 400)
    group by mo.year, mo.movieid 
	order by mo.year desc) as y 
on replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') = y.title2
where mg.movieid  in (
    Select mo.movieid from imdb_movies mo 
    join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    order by year desc limit 400) 
group by title, mo.year , mo.movietitle, x.directors, mo.ratingcount, mo.ratingmean, y.actors
order by mo.year desc, title desc; 



-------------------
-- titulo, géneros y año 
Select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title, STRING_AGG(mg.genre, ', ') as genres,  mo.year
from imdb_moviegenres mg join imdb_movies mo on mg.movieid = mo.movieid 
where mg.movieid  in (
    Select mo.movieid from imdb_movies mo 
    join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    order by year desc limit 400) 
group by title, mo.year
order by mo.year desc, title desc;

 -------------------
 -- dirctores
select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title, STRING_AGG(md.directorname, ', ') as directors
from imdb_directormovies dm join imdb_movies mo on mo.movieid = dm.movieid join imdb_directors md on dm.directorid = md.directorid
where mo.movieid  in (
    Select mo.movieid from imdb_movies mo 
    join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    order by year desc limit 400) 
group by mo.year, mo.movieid 
order by mo.year desc;


----------
-- peliculas relacionadas
Select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title, mo.year, mo.ratingmean from imdb_movies mo 
join imdb_moviegenres mg on mo.movieid = mg.movieid where replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') != 'Autumn Heart' 
and ('Short' = mg.genre or 'Drama' = mg.genre) and mo.movieid in(
    Select mo.movieid from imdb_movies mo 
    join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    order by year desc limit 400) 
group by title, mo.year, mo.ratingmean 
order by year desc limit 10;


---related movies

Select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title, mo.year, mo.ratingmean, STRING_AGG(mg.genre, ', ') as genres
from imdb_moviegenres mg join imdb_movies mo on mg.movieid = mo.movieid where replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') != 'Autumn Heart' 
and mo.movieid in(
    Select mo.movieid from imdb_movies mo 
    join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    order by year desc limit 400) 
	and 'Romance' in (
	
	Select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title, STRING_AGG(mg.genre, ', ') as genres 
from imdb_moviegenres mg join imdb_movies mo on mg.movieid = mo.movieid 
where mg.movieid  in (
    Select mo.movieid from imdb_movies mo 
    join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    order by year desc limit 400) 
group by title, mo.year
order by mo.year desc, title desc


)
group by title, mo.year, mo.ratingmean 
order by year desc limit 10;



Select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title, 
mo.year, mo.ratingcount, STRING_AGG(mg.genre, ', ') as genres
from imdb_moviegenres mg join imdb_movies mo on mg.movieid = mo.movieid 
where replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') != %s  and mg.movieid in (
    Select mo.movieid from imdb_movies mo 
    join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid 
    order by year desc limit 400) 
group by title, mo.year, mo.ratingcount
order by mo.year desc