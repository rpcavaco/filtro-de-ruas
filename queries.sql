
CREATE TABLE codigospostais.codigos_postais
(
	cod_distrito text, cod_concelho text, cod_localidade text, nome_localidade text, cod_arteria text, 
    tipo_arteria text, prep1 text, titulo_arteria text, prep2 text, nome_arteria text, local_arteria text, 
    troco text, porta text, cliente text, num_cod_postal text, ext_cod_postal text, desig_postal text
)

WITH (
    OIDS = FALSE
);

ALTER TABLE codigospostais.codigos_postais
    OWNER to basedata;
    
copy codigospostais.codigos_postais from '/mnt/Dados/DADOS/pg_tablespace/codigos_postais.csv' -- foi pelo pgadmin

delete from codigospostais.codigos_postais where cod_distrito = 'cod_distrito'

create extension unaccent

	with ps as (
		select row_number() over () rn, td.token, td.alias, td.lexemes lx
		from codigospostais.ruasdoporto r, ts_debug(lower(unaccent(r.name))) td,
         
                      where r.id = 10
                      
	) 
	select array_agg(token) into v_tokens
	from ps
	where rn < 4
	and alias != 'blank'
and array_length(lx, 1) > 0;

select * -- row_number() over () rn, td.token, td.alias, td.lexemes lx
from
ts_debug(lower(unaccent(
(select name from codigospostais.ruasdoporto r
where r.id = 48)
    ))) td
    
CREATE OR REPLACE FUNCTION codigospostais.typeparse()
  RETURNS table(id int, tipo text, nomenorm text, nome text) AS
$BODY$
 DECLARE
 	v_row record;
 BEGIN
 	for v_row in (select r.id, r.name 
                 from codigospostais.ruasdoporto r
                 where not name is null)
    loop
    	with ps as (select row_number() over () rn, token, alias, lexemes lx
        from ts_debug(lower(unaccent(v_row.name)))) 
        select string_agg(token, ',') into tipo
        from ps
        where rn < 3
        and alias != 'blank'
        and array_length(lx, 1) > 0;
  
      	with ps as (select row_number() over () rn, token, alias, lexemes lx
        from ts_debug(lower(unaccent(v_row.name)))) 
        select string_agg(token, ' ') into nomenorm
        from ps
        where rn >= 3
        and alias != 'blank'
        and array_length(lx, 1) > 0;

                    nome := v_row.name;
                    id:= v_row.id;
    				return next;
    end loop;
 END
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION codigospostais.typeparse()
OWNER TO BASEDATA;

update codigospostais.ruasdoporto t1
set nomenorm = a.nomenorm,
tipo = a.tipo
from
(select id, tipo, nomenorm from codigospostais.typeparse()) a
where t1.id = a.id

update codigospostais.ruasdoporto t1
set nomenorm = null,
tipo = null

update codigospostais.codigos_postais
set tiponorm = lower(unaccent(tipo_arteria))

CREATE OR REPLACE FUNCTION codigospostais.typeparse_cp()
  RETURNS table(num_cod_postal text, ext_cod_postal text, nomenorm text, nome text) AS
$BODY$
 DECLARE
 	v_row record;
 BEGIN
 	for v_row in (select r.num_cod_postal, r.ext_cod_postal, r.nome_arteria as name
                 from codigospostais.codigos_postais r
                 where not nome_arteria is null
                 and cod_distrito = '13' and cod_concelho = '12')
    loop
      	with ps as (select row_number() over () rn, token, alias, lexemes lx
        from ts_debug(lower(unaccent(v_row.name)))) 
        select string_agg(token, ' ') into nomenorm
        from ps
        where alias != 'blank'
        and array_length(lx, 1) > 0;

                    nome := v_row.name;
                    num_cod_postal:= v_row.num_cod_postal;
                   ext_cod_postal:= v_row.ext_cod_postal;
    				return next;
    end loop;
 END
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION codigospostais.typeparse_cp()
OWNER TO BASEDATA;

select * from codigospostais.typeparse_cp() 

/* nao funciona, num_cod_postal, ext_cod_postal nao é chave
update codigospostais.codigos_postais t1
set nomenorm = a.nomenorm
from
(select num_cod_postal, ext_cod_postal, nomenorm from codigospostais.typeparse_cp()) a
where t1.num_cod_postal = a.num_cod_postal
and t1.ext_cod_postal = a.ext_cod_postal
*/


select num_cod_postal, ext_cod_postal, count(*) 
from codigospostais.typeparse_cp()
group by num_cod_postal, ext_cod_postal

insert into codigospostais.nomes_cp
(nome, tiponorm)
select * from
(select distinct nome_arteria, tiponorm
from codigospostais.codigos_postais r
                 where not nome_arteria is null
                 and cod_distrito = '13' and cod_concelho = '12') a
 order by nome_arteria
 
 update codigospostais.nomes_cp t1
set nomenorm = a.nomenorm
from
(select nome, nomenorm from codigospostais.typeparse_cp()) a
where t1.nome = a.nome

select 'nomes_cp' as tabela, count(*) cnt
from codigospostais.nomes_cp
union all
select 'ruasdoporto' as tabela, count(*) cnt
from codigospostais.ruasdoporto

'nomes_cp','2068'
'ruasdoporto','11311'

select *
from codigospostais.ruasdoporto t1
left outer join codigospostais.nomes_cp t2
on t1.tipo = t2.tiponorm
and t1.nomenorm = t2.nomenorm
where not t1.name is null -- 3153 / 4489 70%


select count(*) from codigospostais.ruasgenero -- 1754


CREATE OR REPLACE FUNCTION codigospostais.typeparse_nomegenero()
  RETURNS table(id integer, nomenorm text, nome text, tipo text) AS
$BODY$
 DECLARE
 	v_row record;
 BEGIN
 	for v_row in (select r.id, r.nome as name
                 from codigospostais.ruasgenero r
                 where not r.nome is null)
    loop
    	with ps as (select row_number() over () rn, token, alias, lexemes lx
        from ts_debug(lower(unaccent(v_row.name)))) 
        select string_agg(token, ' ') into nomenorm
        from ps
        where alias != 'blank'
        and array_length(lx, 1) > 0;

		nome := v_row.name;
                    id := v_row.id;
    				return next;
    end loop;
 END
$BODY$
  LANGUAGE plpgsql VOLATILE;
ALTER FUNCTION codigospostais.typeparse_nomegenero()
OWNER TO BASEDATA;

select * from codigospostais.typeparse_nomegenero()

update codigospostais.ruasgenero t1
set nomenorm = a.nomenorm
from
(select id, nome, nomenorm from codigospostais.typeparse_nomegenero()) a
where t1.id = a.id

update codigospostais.ruasdoporto t1
set sexo = a.sexo
from
(select nomenorm, sexo from codigospostais.ruasgenero) a
where t1.nomenorm = a.nomenorm

