--RELATÓRIO PAGAMENTOS INVERTIDOS (CONECTA FIBRA)
select dr.codigodacidade,
       dr.cliente,
       cl.nome,
       dr.d_datavencimento
	from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
                	cl.codigocliente = dr.cliente and
               	 	dr.d_datapagamento is not null and 
               	 	dr.situacao = 0

left join 
( 
select dr.codigodacidade,
       dr.cliente,
       cl.nome,
       dr.d_datavencimento
	from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
               	 	cl.codigocliente = dr.cliente

	where dr.d_datapagamento is null and
          dr.situacao = 0
) as x on x.codigodacidade = dr.codigodacidade and x.cliente = dr.cliente 

	where date_part('year', dr.d_datavencimento) = date_part('year', CURRENT_DATE) AND 
          date_part('month', dr.d_datavencimento) = date_part('month', CURRENT_DATE) AND
          x.d_datavencimento < TO_DATE(CONCAT('01/',date_part('month', CURRENT_DATE),'/',date_part('year', CURRENT_DATE)), 'DD-MM-YYYY') and
          dr.situacao = 0

----------------------------------------------------------------------------------------------------------------------------------------------------------------

CASE WHEN date_part('month', dr.d_datapagamento) < 10 
	THEN CONCAT(date_part('year', dr.d_datapagamento),'-0',date_part('month', dr.d_datapagamento))
ELSE CONCAT(date_part('year', dr.d_datapagamento),'-',date_part('month', dr.d_datapagamento)) END AS datapgto

---

CREATE OR REPLACE VIEW regrasoperacao.vis_ordem_servico_abertas_tecnet(
    nomecidade,
    codigoassinante,
    idcliente,
    nome,
    tipocliente,
    idcontrato,
    codigocontrato,
    tipodologradouro,
    nomelogradouro,
    numeroconexao,
    bairroconexao,
    complementoconexao,
    numerodoformulario,
    nomevendedor,
    descricaosituacao,
    servico,
    data_atendimento,
    data_agendameanto,
    hora_agendamento,
    bairro,
    equipe,
    motivo_cancelamento,
    usuario_abriu,
    cpf_cnpj,
    macaddress,
    ipconcentrador,
    canal_venda)
AS
  SELECT cid . nomedacidade AS nomecidade,
         cli.id AS codigoassinante,
         ord.codigoassinante AS idcliente,
         cli.nome,
         CASE
           WHEN length(translate(cli.cpf_cnpj::text, '.-/'::text, ''::text)) > 11 THEN 'Pessoa Jurídica'::text
           ELSE 'Pessoa Física'::text
         END AS tipocliente,
         ct.id AS idcontrato,
         ord.codigocontrato,
         ed.tipodologradouro,
         ed.nomelogradouro,
         ct.numeroconexao,
         ct.bairroconexao,
         ct.complementoconexao,
         ct.numerodoformulario,
         vd.nome AS nomevendedor,
         v.descricaosituacao,
         l.descricaodoserv_lanc AS servico,
         ord.d_dataatendimento AS data_atendimento,
         ord.d_dataagendamento AS data_agendameanto,
         ord.t_horaatendimento AS hora_agendamento,
         ct.bairroconexao AS bairro,
         e.nomedaequipe AS equipe,
         m.descmotivo AS motivo_cancelamento,
         ord.atendente AS usuario_abriu,
         cli.cpf_cnpj,
         i.macaddress,
         i.ipconcentrador,
         tv.descricao AS canal_venda
  FROM ordemservico ord
       JOIN lanceservicos l ON l.codigodoserv_lanc = ord.codservsolicitado
       JOIN cidade cid ON cid . codigodacidade = ord.cidade
       JOIN clientes cli ON cli.cidade = ord.cidade AND cli.codigocliente = ord.codigoassinante
       JOIN contratos ct ON ct.cidade = ord.cidade AND ct.codempresa = ord.codempresa AND ct.contrato = ord.codigocontrato
       JOIN equipesdevenda eqv ON eqv.codigo = ct.equipedevenda AND eqv.cidade = ct.cidade
       JOIN vendedores vd ON vd.equipevenda = eqv.codigo AND vd.codigo = ct.vendedor AND vd.cidadeondetrabalha = cid . codigodacidade
       JOIN enderecos ed ON ct.enderecoconexao = ed.codigodologradouro AND cid . codigodacidade = ed.codigodacidade
       JOIN vis_situacaocontrato_descricao v ON v.situacao = ct.situacao
       LEFT JOIN equipe e ON e.codigocidade = ord.cidade AND e.codigodaequipe = ord.equipe
       LEFT JOIN motivocancelamento m ON m.codmotivo = ord.motivocancelamento
       LEFT JOIN tiposdevenda tv ON tv.codigo = ct.tipodevenda
       LEFT JOIN 
       (
         SELECT t.idcliente,
                t.macaddress,
                t.ipconcentrador
         FROM dblink('hostaddr=150.230.79.177 dbname=ins user=postgres password=i745@postgres port=5432'::text,
          'select equ.idcliente, equ.macaddress, equ.ipconcentrador
        from idhcp.equipamentos equ'::text) t(idcliente bigint, macaddress text, ipconcentrador text)
       ) i ON i.idcliente = ct.id
  WHERE cid . codigo_regional = 21 AND
        ord.situacao <> 3;


--FUNÇÃO



BEGIN
      Create temporary table temp_rp_tec_ordens_de_servico_abertas(
          "CIDADE" varchar(30),
          "CODIGO" integer,
          "NOME_CLIENTE" varchar(40),
          "CONTRATO" integer,
          "SITUAÇÃO" text,
          "TIPO" text,
          "ENDEREÇO" text,
          "NUMERO" varchar(10),
          "BAIRRO" varchar(20),
          "COMPLEMENTO" text,
          "FORMULARIO" text,
          "VENDEDOR" varchar(40),
          "SERVIÇO" varchar(40),
          "MOTIVOCANCELAMENTO" varchar(50),
          "ATENDIMENTO" text,
          "AGENDAMENTO" text,
          "HORA-AGENDAMENTO" text,
          "EQUIPE" varchar(30),
          "QUEM_ABRIU" text

    ) On commit drop;
       
        insert into temp_rp_tec_ordens_de_servico_abertas
          select distinct
            os.nomecidade as "Cidade",
            os.codigoassinante as "Código",
            os.nome as "Nome Cliente",
            os.codigocontrato as "Contrato",
            os.descricaosituacao as "Situação",
            os.tipodologradouro as "Tipo",
            os.nomelogradouro as "Endereço",
            os.numeroconexao as "Número",
            os.bairroconexao as "Bairro",
            CASE
                WHEN os.complementoconexao is NULL
                THEN 'SEM COMPLEMENTO' ELSE
                os.complementoconexao END AS "Complemento",
            os.numerodoformulario "Formulário",
            os.nomevendedor as "Vendedor",
            os.servico as "Serviço",
            os.motivo_cancelamento as "Motivo Cancelamento",
            to_char(os.data_atendimento, 'DD/MM/YYYY') as "Atendimento",
            to_char(os.data_agendameanto, 'DD/MM/YYYY') as "Agendamento",
            os.hora_agendamento as "HoraAgendamento",
            CASE
                WHEN os.equipe is null then 'SEM EQUIPE'
                ELSE os.equipe end as "Equipe",
            os.usuario_abriu as "Quem abriu"
          from regrasoperacao.vis_ordem_servico_abertas_tecnet os;
           
        return query select * from temp_rp_tec_ordens_de_servico_abertas;
        
    end;

---

SELECT * FROM regrasoperacao.vis_pagamentos_invertidos

---

CREATE OR REPLACE VIEW regrasoperacao.vis_pagamentos_invertidos(
    codigocidade,
    cliente,
    nome,
    datavencimento)
AS
  SELECT dr.codigodacidade AS codigocidade,
         dr.cliente,
         cl.nome,
         dr.d_datavencimento AS datavencimento
  FROM docreceber dr
       JOIN clientes cl ON cl.cidade = dr.codigodacidade AND 
                     cl.codigocliente = dr.cliente AND 
                     dr.d_datapagamento IS NOT NULL AND 
                     dr.situacao = 0
       LEFT JOIN 
       (
         SELECT dr_1.codigodacidade,
                dr_1.cliente,
                dr_1.d_datavencimento
         FROM docreceber dr_1
              JOIN clientes cl_1 ON cl_1.cidade = dr_1.codigodacidade AND 
                            cl_1.codigocliente = dr_1.cliente
         WHERE dr_1.d_datapagamento IS NULL AND
               dr_1.d_datavencimento <=(CURRENT_DATE - '1 mon'::interval) AND
               dr_1.situacao = 0
       ) x ON x.codigodacidade = dr.codigodacidade AND x.cliente = dr.cliente
  WHERE x.d_datavencimento <= CURRENT_DATE;

---

--GERAR DESCONTO NODES
select * from temporarias.inclui_desconto_horas_node 
('331,261,271,281,351,361',
1160,
'2022-05-01'::date,
'',
'INDISPONIBILIDADE DE TV E INTERNET - 01/05/2022',
1)

---

select cli.id, cli.nome, ct.contrato
from contratos ct
join clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
join contascreditocidade cc on cc.codigocidade=ct.cidade and cc.codigoconta=ct.codcontacredito
where cc.codigocidade = 889601 and ct.situacao <> 5 


select cli.id, cli.nome, ct.contrato
from contratos ct
join clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
join contascreditocidade cc on cc.codigocidade=ct.cidade and cc.codigoconta=ct.codcontacredito
where cc.codigocidade = 889601 and ct.situacao <> 5 --6755 assinantes

---

-- DESCONTOS PROGRAMACAO -- Situação 1 Simulação /  Situação 2 Gerar desconto
select * from temporarias.inclui_desconto_horas_tiraprog()

--Realizar desconto  Endereço -- Situação 1 Simulação /  Situação 2 Gerar desconto
 select * from temporarias.inclui_desconto_horas_endereco2()

---

with 
	x as ( SELECT cliente, numerodocumento, d_dataemissao, d_datavencimento, valordocumento, situacao, valordesconto, valorjuros, valormulta, valorpago, nossonumero
			FROM docreceber )

SELECT x.cliente, cid.nome, x.numerodocumento, x.d_dataemissao, x.d_datavencimento, x.valordocumento, x.situacao, x.valordesconto, x.valorjuros, x.valormulta, x.valorpago, x.nossonumero
from clientes ct

JOIN clientes cli ON cli.nome = ct.nome
join x on x.id = ct.id

---

with 
	x as ( SELECT id, idcidade, nomecliente, username, macaddress
			FROM ins.equipamentos )

SELECT cid.nomedacidade, x.nomecliente, x.username, x.macaddress
from contratos ct


JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
join cidade cid on cid.codigodacidade=cli.cidade
join x on x.id = ct.id

WHERE macaddress IS NOT NULL

--------------------------------------------------------------------------------------------------------------------------------------------

SELECT 	 cid . nomedacidade,
         pr.codigodaprogramacao,
         pr.nomedaprogramacao,
         cp.valorpacote,
         count(*) AS qtde,
         case
         	when ct.tipodocontrato = 11 then 'PADRAO'
            when ct.tipodocontrato = 21 then 'CEMIG'
            when ct.tipodocontrato = 31 then 'PM'
            when ct.tipodocontrato = 41 then 'SCTV'
            when ct.tipodocontrato = 61 then 'SCTV IRMAO'
            when ct.tipodocontrato = 71 then 'PERM'
            when ct.tipodocontrato = 81 then 'FARM.CENTRAL'
            when ct.tipodocontrato = 91 then 'SUPER CANAL'
            when ct.tipodocontrato = 101 then 'PCMG'
            when ct.tipodocontrato = 111 then 'PREFEITURA CTGA'
            when ct.tipodocontrato = 121 then 'TV SISTEC'
            when ct.tipodocontrato = 131 then 'CIVIL'
            when ct.tipodocontrato = 141 then 'PUBLICIDADE'
            when ct.tipodocontrato = 151 then 'PERMUTA'
            when ct.tipodocontrato = 161 then 'CORTESIA'
            when ct.tipodocontrato = 171 then 'PREFEITURAS'
            when ct.tipodocontrato = 181 then 'LOJAS BREDER'
            when ct.tipodocontrato = 191 then 'CORP.SCTV'
            when ct.tipodocontrato = 201 then 'MIGRAÇÃO'
            when ct.tipodocontrato = 211 then 'CX ESCOLAR'
            when ct.tipodocontrato = 221 then 'DPC'
         end as "tipo_contrato",
         
         CASE
	WHEN ct.situacao = 1 then 'Aguard. Conexão'::text
    WHEN ct.situacao = 2 then 'ConectadoAtivo'::text
    WHEN ct.situacao = 3 then 'Pausado'::text
    WHEN ct.situacao = 4 then 'Inadimplente'::text
    WHEN ct.situacao = 5 then 'Cancelado'::text
    WHEN ct.situacao = 6 then 'EndereçoNaoCabeado'::text 
    WHEN ct.situacao = 7 then 'ConectadoInadimplente'::text
END AS situacaocontrato
         
  FROM cont_prog cp
       JOIN contratos ct ON ct.cidade = cp.cidade AND ct.codempresa =
         cp.codempresa AND ct.contrato = cp.contrato
       JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente =
         ct.codigodocliente
       JOIN cidade cid ON cid . codigodacidade = ct.cidade
       JOIN programacao pr ON pr.codcidade = cp.cidade AND
         pr.codigodaprogramacao = cp.protabelaprecos

  GROUP BY cid . nomedacidade,
           pr.codigodaprogramacao,
           pr.nomedaprogramacao,
           cp.valorpacote,
           ct.tipodocontrato,
           ct.situacao

  ORDER BY cid . nomedacidade,t
           pr.codigodaprogramacao,
           pr.nomedaprogramacao,
           cp.valorpacote

--------------------------------------------------------------------------------------------------------------------------------------------

FUNÇÃOLIBERA ACESSO

SELECT * 
FROM func_libera_acesso('group_imanager')

------------------------------------------------------------------------------------------------------------------------------------------

with 
	x as ( SELECT cliente, nomecliente, numerodocumento, d_dataemissao, d_datavencimento, valordocumento, situacao, valordesconto, valorjuros, valormulta, valorpago, nossonumero
			FROM docreceber )

SELECT x.cliente, nome, x.numerodocumento, x.d_dataemissao, x.d_datavencimento, x.valordocumento, x.situacao, x.valordesconto, x.valorjuros, x.valormulta, x.valorpago, x.nossonumero

from clientes


JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
join cidade cid on cid.codigodacidade=cli.cidade
join x on x.id = ct.id

WHERE macaddress IS NOT NULL

------------------------------------------------------------------------------------------------------------------------------------------

CONECTA FIBRA

SELECT cliente, numerodocumento, d_dataemissao, d_datavencimento, valordocumento, situacao, valordesconto, valorjuros, valormulta, valorpago, nossonumero

FROM docreceber

limit 10

SELECT *
FROM clientes
limit 10

cliente, numerodocumento, d_dataemissao, d_datavencimento, valordocumento, situacao, valordesconto, valorjuros, valormmulta, valorpago, nossonumero
where

------------------------------------------------------------------------------------------------------------------------------------------

select distinct * from (
select DISTINCT cid.nomedacidade, cli.codigocliente, cli.nome,ct.contrato,ct.id,
CASE
	WHEN ct.situacao = 1 then 'Aguard. Conexão'::text
    WHEN ct.situacao = 2 then 'ConectadoAtivo'::text
    WHEN ct.situacao = 3 then 'Pausado'::text
    WHEN ct.situacao = 4 then 'Inadimplente'::text
    WHEN ct.situacao = 5 then 'Cancelado'::text
    WHEN ct.situacao = 6 then 'EndereçoNaoCabeado'::text 
    WHEN ct.situacao = 7 then 'ConectadoInadimplente'::text
END AS situacaocontrato,
x.tipoequipamento,
x.dataretirada,
x.id as "id.equipamento",
x.macaddress,
x.chassis,
x.slot,
x.porta,
x.serial,
x.datadesativacaomac
from cont_prog cp
left join cidade cid on cp.cidade = cid.codigodacidade
left join contratos ct on ct.cidade = cp.cidade and ct.contrato = cp.contrato
left join clientes cli on cli.cidade = cp.cidade and ct.codigodocliente = cli.codigocliente
left JOIN 
       (
         SELECT i.idcliente,
                i.tipoequipamento,
                i.dataretirada,
                i.id,
                i.macaddress,
                i.chassis,
                i.slot,
                i.porta,
                i.serial,
                i.datadesativacaomac
                
         FROM dblink(
           'hostaddr=187.63.192.133 port=5432 user=postgres password=i745@postgres dbname=ins'
           ::text, '
          select distinct  eq.idcliente,eq.macaddress,c.descricao as desc_chassi, ol.descricao as desc_slot,eq.portaoltchassis,
          case 
          when eq.tipoequipamento  = 9 then ''ONU''
          when eq.tipoequipamento = 7 then ''CPE RADIUS''
          end as tipoeq,
          eq.d_dataretirada,eq.id, eq.serial, eq.d_datadesativacaomac
          from idhcp.equipamentos eq
          left join idhcp.oltslot ol on ol.id = eq.idoltslot
          left join idhcp.oltchassis c on c.id = ol.idoltchassis 
          group by eq.idcliente,eq.macaddress,ol.descricao,c.descricao,eq.portaoltchassis,eq.ipconcentrador,eq.tipoequipamento,eq.d_dataretirada, eq.id
        '::text) i(idcliente bigint, macaddress text, chassis text, slot text, porta integer, tipoequipamento text,dataretirada date, id integer, serial text, datadesativacaomac date )
       ) x ON x.idcliente = ct.id
  where  x.id is not null
  
   union
       
       select DISTINCT cid.nomedacidade, cli.codigocliente, cli.nome,ct.contrato,ct.id,
CASE
	WHEN ct.situacao = 1 then 'Aguard. Conexão'::text
    WHEN ct.situacao = 2 then 'ConectadoAtivo'::text
    WHEN ct.situacao = 3 then 'Pausado'::text
    WHEN ct.situacao = 4 then 'Inadimplente'::text
    WHEN ct.situacao = 5 then 'Cancelado'::text
    WHEN ct.situacao = 6 then 'EndereçoNaoCabeado'::text 
    WHEN ct.situacao = 7 then 'ConectadoInadimplente'::text
END AS situacaocontrato,
x.tipoequipamento,
x.dataretirada,
x.id as "id.equipamento",
x.macaddress,
x.chassis,
x.slot,
x.porta,
x.serial,
x.datadesativacaomac
from cont_prog cp
left join cidade cid on cp.cidade = cid.codigodacidade
left join contratos ct on ct.cidade = cp.cidade and ct.contrato = cp.contrato
left join clientes cli on cli.cidade = cp.cidade and ct.codigodocliente = cli.codigocliente
left JOIN 
       (
          SELECT i.idcliente,
                i.tipoequipamento,
                i.dataretirada,
                i.id,
                i.macaddress,
                i.chassis,
                i.slot,
                i.porta,
                i.serial,
                i.datadesativacaomac
                
         FROM dblink(
           'hostaddr=177.129.48.5 port=5432 user=postgres password=i745@postgres dbname=ins'
           ::text, '
          select distinct  eq.idcliente,eq.macaddress,c.descricao as desc_chassi, ol.descricao as desc_slot,eq.portaoltchassis,
          case 
          when eq.tipoequipamento  = 9 then ''ONU''
          when eq.tipoequipamento  = 7 then ''CPE RADIUS''
          end as tipoeq,
          eq.d_dataretirada,eq.id, eq.serial, eq.d_datadesativacaomac
          from idhcp.equipamentos eq
          left join idhcp.oltslot ol on ol.id = eq.idoltslot
          left join idhcp.oltchassis c on c.id = ol.idoltchassis 
          group by eq.idcliente,eq.macaddress,ol.descricao,c.descricao,eq.portaoltchassis,eq.ipconcentrador,eq.tipoequipamento,eq.d_dataretirada, eq.id
        '::text) i(idcliente bigint, macaddress text, chassis text, slot text, porta integer, tipoequipamento text,dataretirada date, id integer, serial text, datadesativacaomac date )
       ) x ON x.idcliente = ct.id
  where  x.id is not null) as x
  limit 100

------------------------------------------------------------------------------------------------------------------------------------------

--UPDATE troca conta credito contrato
update contratos ct set codcontacredito = 91 where id in (
select ct.id
from contratos ct
join clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
join contascreditocidade cc on cc.codigocidade=ct.cidade and cc.codigoconta=ct.codcontacredito
where cc.codigocidade = 889601 and ct.situacao <> 5 );  --6755 assinante

update clientes cli set codcontacredito = 91 where id in (
select cli.id
from contratos ct
join clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
join contascreditocidade cc on cc.codigocidade=ct.cidade and cc.codigoconta=ct.codcontacredito
where cc.codigocidade = 889601 and ct.situacao <> 5 )

------------------------------------------------------------------------------------------------------------------------------------------

SELECT 	 cid . nomedacidade,
         pr.codigodaprogramacao,
         pr.nomedaprogramacao,
         cp.valorpacote,
         count(*) AS qtde,
         case
         	when ct.tipodocontrato = 11 then 'PADRAO'
            when ct.tipodocontrato = 21 then 'CEMIG'
            when ct.tipodocontrato = 31 then 'PM'
            when ct.tipodocontrato = 41 then 'SCTV'
            when ct.tipodocontrato = 61 then 'SCTV IRMAO'
            when ct.tipodocontrato = 71 then 'PERM'
            when ct.tipodocontrato = 81 then 'FARM.CENTRAL'
            when ct.tipodocontrato = 91 then 'SUPER CANAL'
            when ct.tipodocontrato = 101 then 'PCMG'
            when ct.tipodocontrato = 111 then 'PREFEITURA CTGA'
            when ct.tipodocontrato = 121 then 'TV SISTEC'
            when ct.tipodocontrato = 131 then 'CIVIL'
            when ct.tipodocontrato = 141 then 'PUBLICIDADE'
            when ct.tipodocontrato = 151 then 'PERMUTA'
            when ct.tipodocontrato = 161 then 'CORTESIA'
            when ct.tipodocontrato = 171 then 'PREFEITURAS'
            when ct.tipodocontrato = 181 then 'LOJAS BREDER'
            when ct.tipodocontrato = 191 then 'CORP.SCTV'
            when ct.tipodocontrato = 201 then 'MIGRAÇÃO'
            when ct.tipodocontrato = 211 then 'CX ESCOLAR'
            when ct.tipodocontrato = 221 then 'DPC'
         end as "tipo_contrato",
         
         CASE
	WHEN ct.situacao = 1 then 'Aguard. Conexão'::text
    WHEN ct.situacao = 2 then 'ConectadoAtivo'::text
    WHEN ct.situacao = 3 then 'Pausado'::text
    WHEN ct.situacao = 4 then 'Inadimplente'::text
    WHEN ct.situacao = 5 then 'Cancelado'::text
    WHEN ct.situacao = 6 then 'EndereçoNaoCabeado'::text 
    WHEN ct.situacao = 7 then 'ConectadoInadimplente'::text
END AS situacaocontrato
         
  FROM cont_prog cp
       JOIN contratos ct ON ct.cidade = cp.cidade AND ct.codempresa =
         cp.codempresa AND ct.contrato = cp.contrato
       JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente =
         ct.codigodocliente
       JOIN cidade cid ON cid . codigodacidade = ct.cidade
       JOIN programacao pr ON pr.codcidade = cp.cidade AND
         pr.codigodaprogramacao = cp.protabelaprecos

  GROUP BY cid . nomedacidade,
           pr.codigodaprogramacao,
           pr.nomedaprogramacao,
           cp.valorpacote,
           ct.tipodocontrato,
           ct.situacao

  ORDER BY cid . nomedacidade,
           pr.codigodaprogramacao,
           pr.nomedaprogramacao,
           cp.valorpacote

------------------------------------------------------------------------------------------------------------------------------------------

with 
	x as ( SELECT id, idcidade, nomecliente, username, macaddress
			FROM ins.equipamentos )

SELECT cid.nomedacidade, x.nomecliente, x.username, x.macaddress
from contratos ct


JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
join cidade cid on cid.codigodacidade=cli.cidade
join x on x.id = ct.id

WHERE macaddress IS NOT NULL

------------------------------------------------------------------------------------------------------------------------------------------

with 
	x as ( SELECT cliente, numerodocumento, d_dataemissao, d_datavencimento, valordocumento, situacao, valordesconto, valorjuros, valormulta, valorpago, nossonumero
			FROM docreceber )

SELECT x.cliente, cid.nome, x.numerodocumento, x.d_dataemissao, x.d_datavencimento, x.valordocumento, x.situacao, x.valordesconto, x.valorjuros, x.valormulta, x.valorpago, x.nossonumero
from clientes ct

JOIN clientes cli ON cli.nome = ct.nome
join x on x.id = ct.id

------------------------------------------------------------------------------------------------------------------------------------------

-- DESCONTOS PROGRAMACAO -- Situação 1 Simulação /  Situação 2 Gerar desconto
select * from temporarias.inclui_desconto_horas_tiraprog()

--Realizar desconto  Endereço -- Situação 1 Simulação /  Situação 2 Gerar desconto
 select * from temporarias.inclui_desconto_horas_endereco2()

------------------------------------------------------------------------------------------------------------------------------------------

select cli.id, cli.nome, ct.contrato
from contratos ct
join clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
join contascreditocidade cc on cc.codigocidade=ct.cidade and cc.codigoconta=ct.codcontacredito
where cc.codigocidade = 889601 and ct.situacao <> 5 


select cli.id, cli.nome, ct.contrato
from contratos ct
join clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
join contascreditocidade cc on cc.codigocidade=ct.cidade and cc.codigoconta=ct.codcontacredito
where cc.codigocidade = 889601 and ct.situacao <> 5 --6755 assinantes

------------------------------------------------------------------------------------------------------------------------------------------

--CÓDIGO PAGAMENTOS INVERTIDOS CONECTA FIBRA (OSWALDO CRUZ)
with x as(
         select dr.codigodacidade,
        		dr.cliente,
                cl.nome
	from docreceber dr
    		  join public.clientes cl on cl.cidade = dr.codigodacidade and
                cl.codigocliente = dr.cliente
		where dr.d_datavencimento BETWEEN '2022-05-01' and '2022-05-31' and 
                dr.d_datapagamento is not null and
                dr.situacao = 0),
               
 y as ( 
 select dr.codigodacidade,
        dr.cliente
 	from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
                 cl.codigocliente = dr.cliente
 		where dr.d_datavencimento <= '2022-04-30' and 
                 dr.d_datapagamento is  null and
                 dr.situacao = 0)
                            
 select x.* from x
 join y on y.codigodacidade = x.codigodacidade and y.cliente = x.cliente

------------------------------------------------------------------------------------------------------------------------------------------

--GERAR DESCONTO NODES
select * from temporarias.inclui_desconto_horas_node 
('331,261,271,281,351,361',
1160,
'2022-05-01'::date,
'',
'INDISPONIBILIDADE DE TV E INTERNET - 01/05/2022',
1)


------------------------------------------------------------------------------------------------------------------------------------------

->Enceremento depois de muito tempo sem retorno:
Olá! Como vai?

Informo que devido a falta de interação estarei encerrando este ticket.
Porém, caso haja necessidade de interagir novamente, peço para que abra um novo chamado!
Desde já agradecemos o contato, para quaisquer eventualidades estaremos à total disposição!

Teremos o maior prazer em lhe auxiliar! =)

------------------------------------------------------------------------------------------------------------------------------------------
--VIEW ERRADA
/*CREATE VIEW regrasoperacao.vis_pagamentos_invertidos ( 
	codigocidade,
    cliente,
    nome,
    datavencimento)
     
AS
 
SELECT with x as(
         select dr.codigodacidade,
                dr.cliente,
                cl.nome,
                dr.d_datavencimento
from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
                cl.codigocliente = dr.cliente
where          dr.d_datapagamento is not null and
               dr.situacao = 0),
               
 y as (  
 select dr.codigodacidade,
        dr.cliente 
 from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
                cl.codigocliente = dr.cliente
 where dr.d_datavencimento <= current_date - interval '1 month'  and 
               dr.d_datapagamento is  null and
               dr.situacao = 0)
                                              
 select x.* from x
 join y on y.codigodacidade = x.codigodacidade and y.cliente = x.cliente;*/

------------------------------------------------------------------------------------------------------------------------------------------
--VIEW ERRADA
/*
select 			dr.codigodacidade,
                dr.cliente,
                cl.nome,
                dr.d_datavencimento
from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
                cl.codigocliente = dr.cliente and dr.d_datapagamento is not null and dr.situacao = 0
      
left join 
( 
select dr.codigodacidade,
       dr.cliente,
       dr.d_datavencimento
from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
                cl.codigocliente = dr.cliente
where dr.d_datapagamento is null and dr.d_datavencimento <= current_date - interval '1 month' and
      dr.situacao = 0
) as x on x.codigodacidade = dr.codigodacidade and x.cliente = dr.cliente 
where dr.d_datavencimento BETWEEN '2022-05-01' and '2022-05-31' and
      x.d_datavencimento <= current_date*/

------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW regrasoperacao.vis_pagamentos_invertidos(
    codigocidade,
    cliente,
    nome,
    datavencimento)
AS
  SELECT dr.codigodacidade AS codigocidade,
         dr.cliente,
         cl.nome,
         dr.d_datavencimento AS datavencimento
  FROM docreceber dr
       JOIN clientes cl ON cl.cidade = dr.codigodacidade AND 
                     cl.codigocliente = dr.cliente AND 
                     dr.d_datapagamento IS NOT NULL AND 
                     dr.situacao = 0
       LEFT JOIN 
       (
         SELECT dr_1.codigodacidade,
                dr_1.cliente,
                dr_1.d_datavencimento,
         FROM docreceber dr_1
              JOIN clientes cl_1 ON cl_1.cidade = dr_1.codigodacidade AND 
                            cl_1.codigocliente = dr_1.cliente
         WHERE dr_1.d_datapagamento IS NULL AND
               dr_1.d_datavencimento <=(CURRENT_DATE - '1 mon'::interval) AND
               dr_1.situacao = 0
       ) x ON x.codigodacidade = dr.codigodacidade AND x.cliente = dr.cliente
  WHERE x.d_datavencimento <= CURRENT_DATE;

------------------------------------------------------------------------------------------------------------------------------------------

SELECT * FROM regrasoperacao.vis_pagamentos_invertidos

------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW regrasoperacao.vis_ordem_servico_abertas_tecnet(
    nomecidade,
    codigoassinante,
    idcliente,
    nome,
    tipocliente,
    idcontrato,
    codigocontrato,
    tipodologradouro,
    nomelogradouro,
    numeroconexao,
    bairroconexao,
    complementoconexao,
    numerodoformulario,
    nomevendedor,
    descricaosituacao,
    servico,
    data_atendimento,
    data_agendameanto,
    hora_agendamento,
    bairro,
    equipe,
    motivo_cancelamento,
    usuario_abriu,
    cpf_cnpj,
    macaddress,
    ipconcentrador,
    canal_venda)
AS
  SELECT cid . nomedacidade AS nomecidade,
         cli.id AS codigoassinante,
         ord.codigoassinante AS idcliente,
         cli.nome,
         CASE
           WHEN length(translate(cli.cpf_cnpj::text, '.-/'::text, ''::text)) > 11 THEN 'Pessoa Jurídica'::text
           ELSE 'Pessoa Física'::text
         END AS tipocliente,
         ct.id AS idcontrato,
         ord.codigocontrato,
         ed.tipodologradouro,
         ed.nomelogradouro,
         ct.numeroconexao,
         ct.bairroconexao,
         ct.complementoconexao,
         ct.numerodoformulario,
         vd.nome AS nomevendedor,
         v.descricaosituacao,
         l.descricaodoserv_lanc AS servico,
         ord.d_dataatendimento AS data_atendimento,
         ord.d_dataagendamento AS data_agendameanto,
         ord.t_horaatendimento AS hora_agendamento,
         ct.bairroconexao AS bairro,
         e.nomedaequipe AS equipe,
         m.descmotivo AS motivo_cancelamento,
         ord.atendente AS usuario_abriu,
         cli.cpf_cnpj,
         i.macaddress,
         i.ipconcentrador,
         tv.descricao AS canal_venda
  FROM ordemservico ord
       JOIN lanceservicos l ON l.codigodoserv_lanc = ord.codservsolicitado
       JOIN cidade cid ON cid . codigodacidade = ord.cidade
       JOIN clientes cli ON cli.cidade = ord.cidade AND cli.codigocliente = ord.codigoassinante
       JOIN contratos ct ON ct.cidade = ord.cidade AND ct.codempresa = ord.codempresa AND ct.contrato = ord.codigocontrato
       JOIN equipesdevenda eqv ON eqv.codigo = ct.equipedevenda AND eqv.cidade = ct.cidade
       JOIN vendedores vd ON vd.equipevenda = eqv.codigo AND vd.codigo = ct.vendedor AND vd.cidadeondetrabalha = cid . codigodacidade
       JOIN enderecos ed ON ct.enderecoconexao = ed.codigodologradouro AND cid . codigodacidade = ed.codigodacidade
       JOIN vis_situacaocontrato_descricao v ON v.situacao = ct.situacao
       LEFT JOIN equipe e ON e.codigocidade = ord.cidade AND e.codigodaequipe = ord.equipe
       LEFT JOIN motivocancelamento m ON m.codmotivo = ord.motivocancelamento
       LEFT JOIN tiposdevenda tv ON tv.codigo = ct.tipodevenda
       LEFT JOIN 
       (
         SELECT t.idcliente,
                t.macaddress,
                t.ipconcentrador
         FROM dblink('hostaddr=150.230.79.177 dbname=ins user=postgres password=i745@postgres port=5432'::text,
          'select equ.idcliente, equ.macaddress, equ.ipconcentrador
        from idhcp.equipamentos equ'::text) t(idcliente bigint, macaddress text, ipconcentrador text)
       ) i ON i.idcliente = ct.id
  WHERE cid . codigo_regional = 21 AND
        ord.situacao <> 3;


--FUNÇÃO



BEGIN
      Create temporary table temp_rp_tec_ordens_de_servico_abertas(
          "CIDADE" varchar(30),
          "CODIGO" integer,
          "NOME_CLIENTE" varchar(40),
          "CONTRATO" integer,
          "SITUAÇÃO" text,
          "TIPO" text,
          "ENDEREÇO" text,
          "NUMERO" varchar(10),
          "BAIRRO" varchar(20),
          "COMPLEMENTO" text,
          "FORMULARIO" text,
          "VENDEDOR" varchar(40),
          "SERVIÇO" varchar(40),
          "MOTIVOCANCELAMENTO" varchar(50),
          "ATENDIMENTO" text,
          "AGENDAMENTO" text,
          "HORA-AGENDAMENTO" text,
          "EQUIPE" varchar(30),
          "QUEM_ABRIU" text

    ) On commit drop;
       
        insert into temp_rp_tec_ordens_de_servico_abertas
          select distinct
            os.nomecidade as "Cidade",
            os.codigoassinante as "Código",
            os.nome as "Nome Cliente",
            os.codigocontrato as "Contrato",
            os.descricaosituacao as "Situação",
            os.tipodologradouro as "Tipo",
            os.nomelogradouro as "Endereço",
            os.numeroconexao as "Número",
            os.bairroconexao as "Bairro",
            CASE
                WHEN os.complementoconexao is NULL
                THEN 'SEM COMPLEMENTO' ELSE
                os.complementoconexao END AS "Complemento",
            os.numerodoformulario "Formulário",
            os.nomevendedor as "Vendedor",
            os.servico as "Serviço",
            os.motivo_cancelamento as "Motivo Cancelamento",
            to_char(os.data_atendimento, 'DD/MM/YYYY') as "Atendimento",
            to_char(os.data_agendameanto, 'DD/MM/YYYY') as "Agendamento",
            os.hora_agendamento as "HoraAgendamento",
            CASE
                WHEN os.equipe is null then 'SEM EQUIPE'
                ELSE os.equipe end as "Equipe",
            os.usuario_abriu as "Quem abriu"
          from regrasoperacao.vis_ordem_servico_abertas_tecnet os;
           
        return query select * from temp_rp_tec_ordens_de_servico_abertas;
        
    end;

------------------------------------------------------------------------------------------------------------------------------------------

CASE WHEN date_part('month', dr.d_datapagamento) < 10 
	THEN CONCAT(date_part('year', dr.d_datapagamento),'-0',date_part('month', dr.d_datapagamento))
ELSE CONCAT(date_part('year', dr.d_datapagamento),'-',date_part('month', dr.d_datapagamento)) END AS datapgto

------------------------------------------------------------------------------------------------------------------------------------------

select dr.codigodacidade,
       dr.cliente,
       cl.nome,
       dr.d_datavencimento
	from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
                	cl.codigocliente = dr.cliente and
               	 	dr.d_datapagamento is not null and 
               	 	dr.situacao = 0

left join 
( 
select dr.codigodacidade,
       dr.cliente,
       cl.nome,
       dr.d_datavencimento
	from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
               	 	cl.codigocliente = dr.cliente

	where dr.d_datapagamento is null and
          dr.situacao = 0
) as x on x.codigodacidade = dr.codigodacidade and x.cliente = dr.cliente 

	where date_part('year', dr.d_datavencimento) = date_part('year', CURRENT_DATE) AND 
          date_part('month', dr.d_datavencimento) = date_part('month', CURRENT_DATE) AND
          x.d_datavencimento < TO_DATE(CONCAT('01/',date_part('month', CURRENT_DATE),'/',date_part('year', CURRENT_DATE)), 'DD-MM-YYYY') and
          dr.situacao = 0

------------------------------------------------------------------------------------------------------------------------------------------

-> 
Conforme solicitação finalizada, estaremos encerrando este ticket.


Porém, caso haja necessidade de interagir novamente, peço para que abra um novo chamado referenciando o mesmo!
Desde já agradecemos o contato, para quaisquer eventualidades estaremos à total disposição!


Teremos o maior prazer em lhe auxiliar! =)

------------------------------------------------------------------------------------------------------------------------------------------

--RELATÓRIO PERSONALIZADO (Pagamentos Invertidos) OK

--CABEÇALHO
Cidade;Código;Nome;Contrato;Data Pagamento;Data Vencimento Aberto

--SELECT
with 
  aberto as (   
     SELECT distinct dr_1.codigodacidade, dr_1.cliente, cl_1.nome, dr_1.d_datavencimento, m.contrato
     FROM docreceber dr_1
     join movimfinanceiro m on m.numfatura=dr_1.fatura
     JOIN clientes cl_1 ON cl_1.cidade = dr_1.codigodacidade AND cl_1.codigocliente = dr_1.cliente
     WHERE dr_1.d_datapagamento IS NULL AND dr_1.situacao = 0
  )
   select distinct cid.nomedacidade, cl.codigocliente, cl.nome, m.contrato, dr.d_datapagamento, a.d_datavencimento
   from docreceber dr
   join movimfinanceiro m on m.numfatura=dr.fatura
   JOIN clientes cl ON cl.cidade = dr.codigodacidade AND cl.codigocliente = dr.cliente
   join cidade cid on cid.codigodacidade=dr.codigodacidade
   join aberto a on a.codigodacidade=dr.codigodacidade and a.cliente=dr.cliente and a.contrato=m.contrato
   left join aberto aa on aa.codigodacidade=dr.codigodacidade and aa.cliente=dr.cliente and aa.contrato=m.contrato

--WHERE
where dr.d_datapagamento between pDataInicial and pDataFinal
   and a.d_datavencimento < pDataInicial
   and dr.d_datavencimento > aa.d_datavencimento

------------------------------------------------------------------------------------------------------------------------------------------

--PEGAR INFORMAÇÕES/ENREDEÇO DE BOLETO CLIENTE
select * from central.conta

------------------------------------------------------------------------------------------------------------------------------------------

--PEGA DATA DO MES E ANO
to_char(current_date,'YYYY-MM-01')::date

------------------------------------------------------------------------------------------------------------------------------------------

SELECT rj.codigodacidade AS codigo_cidade,
         c.descricao AS carteira,
         rj.nomedacidade AS cidade_nome,
         rj.codigocliente AS cliente_codigo,
         rj.nome AS cliente_nome,
         rj.contrato AS contrato_codigo,
         rj.situacao_contrato AS contrato_situacao,
         rj.d_data::date AS data_reajuste,
         rj.codigodaprogramacao AS programacao_codigo,
         rj.nomedaprogramacao AS programacao_nome,
         rj.d_datadainstalacaocontrato AS contrato_instalacao,
         rj.d_dataativacaoprogramacao AS pacote_ativacao,
         rj.d_dataalttabelaprogramacao AS pacote_ultimo_reajuste,
         rj.valoratualpacote::numeric (15, 2) AS pacote_valor_atual,
         rj.valorpacotereajustado::numeric (15, 2) AS pacote_valor_reajustado,
         concat("substring"(rj.descricaoreajuste, 38, 5), '%') AS reajuste_indice,
         rj.contrato_id,
         rj.cont_prog_id AS pacote_id, 
         round(rj.valorpacotereajustado - rj.valoratualpacote,2) AS aumento,
         to_char(ct.d_datadainstalacao::timestamp with time zone, 'MM/YY'::text) AS periodo
  FROM reajustesefetivados rj
       JOIN contratos ct ON ct.id = rj.contrato_id
       JOIN carteira c ON c.codigo = ct.codcarteira

where rj.d_data BETWEEN '2021-05-01' and '2021-05-31'

------------------------------------------------------------------------------------------------------------------------------------------

--BOLETO MENORES QUE R$10
select c.nomedacidade as "Cidade",
cl.nome as "Nome_Assinante",
dr.numerodocumento as "Num_Documento",
dr.valordocumento as "Valor_Documento",
dr.d_datavencimento as "Data_Vencimento",
dr.id as "Id_Boleto"
from docreceber dr
join cidade c on c.codigodacidade = dr.codigodacidade
join public.clientes cl on cl.cidade = dr.codigodacidade and
cl.codigocliente = dr.cliente
where dr.valordocumento < 10.00 and
dr.situacao = 0 and
dr.formadepagamento = 1 and
dr.d_datapagamento is null
order by cl.nome

------------------------------------------------------------------------------------------------------------------------------------------

-- UPDATE PARA LIMPAR URL BOLETO
update docreceber set url_pdf_terceiros = NULL, arquivopdf = null
from (
  select dr.id, dr.d_datafaturamento, dr.url_pdf_terceiros
  from docreceber dr
  where dr.formadepagamento = 1 and dr.situacao = 0 and dr.d_datavencimento between '2022-06-01' and '2022-06-30' 
  and dr.d_datafaturamento = '2022-05-31' and dr.url_pdf_terceiros is not null
  AND dr.id = 2017393 --(APENAS PARA ESSE ID DE BOLETO) 
)
sql
where sql.id = docreceber.id

-- TRAZ BOLETO SEM URL
select dr.url_pdf_terceiros, * from docreceber dr where dr.d_datapagamento is null and dr.d_datavencimento > '2022-06-01'
and dr.url_pdf_terceiros is null

-- TRAZER ID BOLETO
select * from docreceber dr
where dr.nossonumero = 21476641

------------------------------------------------------------------------------------------------------------------------------------------

--MOVIMENTAÇÃO DE PRODUTO POR ARMAZÉM

--CABEÇALHOS
datamovimentacao;tipomovimentacao;numeromiventacao;armazemorigem;armazemdestino;codigoproduto;descricaoproduto;nomeservico;ordemservico;contrato;quantidade

select t.* from (
  select datamovimentacao, tipomovimentacao, numeromiventacao, armazemorigem, armazemdestino,
    codigoproduto, descricaoproduto, nomeservico, ordemservico, contrato, quantidade
  from intranet.estoque_movimentacao_periodo([dataInicio],[dataFim])
) as t

------------------------------------------------------------------------------------------------------------------------------------------

select distinct 
os.carteira as "Carteira", 
os.nomecidade as "Cidade", 
os.codigoassinante as "Código", 
os.nome as "Nome Cliente", 
os.codigocontrato as "Contrato",
os.descricaosituacao as "Situação", 
os.numos as "Nº OS", 
os.servico as "Serviço", 
os.data_atendimento as "Atendimento", to_char(os.horaatendimento,'HH24:MM') as "Hora Atendimento",
os.data_agendameanto as "Agendamento", 
os.data_execucao as "Execução", to_char(os.horaexecucao,'HH24:MM') as "Hora Execução",
os.data_realbaixa as "Data Real Baixa", 
os.bairro as "Bairro", 
os.equipe as "Equipe", 
os.equipeexecutou as "Equipe executou", 
os.tipo as "Tipo",
os.motivo_cancelamento as "Motivo Cancelamento", 
os.usuario_abriu as "Usuario que Abriu", 
os.vendedor as "Vendedor", 
os.pacote as "Pacote", 
os.valor_pacote as "Valor_Pacote",
os.endereco as "Endereço", 
os.numeroconexao as "Nº Conexão", 
os.aptoconexao as "Apto", 
os.blococonexao as "Bloco", 
os.bairroconexao as "Bairro", 
os.idcontrato as "Id Contrato",
os.ocorrencias as "Ocorrências"

from regrasoperacao.vis_ordem_servico_planos_conexao os

where os.carteira = 'WEBNET' AND OS.data_agendameanto is not null

------------------------------------------------------------------------------------------------------------------------------------------

select distinct 
       os.nomecidade,
       os.codigoassinante,
       os.nome,
       os.cpf_cnpj,
       os.codigocontrato,
       os.tipocontrato,
       os.descricaosituacao,
       os.numos,
       os.servico,
       os.data_atendimento,
       os.horaatendimento, 
       os.data_agendameanto,
       os.data_execucao,
       os.horaexecucao,
       os.horafinal, 
       os.data_realbaixa,
       os.equipe,
       os.equipeexecutou,
       os.tecnologia,
       os.tipo,
       os.motivo_cancelamento,
       os.usuario_abriu,
       os.grupo_usuario,
       os.vendedor,
       os.pacote,
       os.valor_pacote,
       os.valor_pacote_desconto,
       os.endereco,
       os.numeroconexao,
       os.aptoconexao,
       os.blococonexao,
       os.bairroconexao,
       os.idcontrato,
       os.ocorrencias,
       os.carteira,
       os.data_ativacao, 
       os.canal_venda, 
       os.equipe_venda
       from regrasoperacao.vis_ordem_servico_planos_conexao os
       
       
       where os.data_agendameanto is not null and os.carteira = 'WEBNET'

------------------------------------------------------------------------------------------------------------------------------------------

--UPDATE TIPOS DE CONTRATOS
update contratos
set tipodocontrato = 21
where contrato in (1628280, 1628277, 1628279, 1628278, 1628235, 1480000, 1628685, 1554735, 1554734, 
                   1564204, 1561469, 1564266, 1564169, 1557049, 1554732, 1554761, 1554760, 1554743, 
                   1562716, 1561488, 1628157, 1498694, 1566616, 1628283, 1628112, 1628096, 1635540, 
                   1483412, 1635105, 1628694,  1564110, 1504375, 1628116, 1628681, 1565680);


update contratos
set tipodocontrato = 71
where contrato in (1628102, 1468987, 1561400, 1628816, 1628483, 1628227, 1628709, 1628537, 1628423, 1538242, 1503351, 1528547, 1483369, 1628981,
  1483540, 1628281, 1628303, 1508037, 1628977, 1628301, 1628103, 1507210, 1628586, 1628286, 1628305, 1479724, 1628312, 1509982, 1541052, 1553011,
  1468692, 1628220, 1541484, 1628457, 1532382, 1628605, 1484064, 1628098, 1628573, 1560262, 1628517, 1628549, 1581271, 1628571, 1628714, 1628572,
  1628550, 1635505, 1504324, 1635073, 1541134, 1628250, 1545486, 1628711, 1504465, 1628814, 1628285, 1628607, 1628443, 1495844, 1628294, 1542896,
  1635152, 1553530, 1627990, 1628210, 1628257, 1628160, 1628158, 1628292, 1466933, 1628784, 1628100, 1505758, 1507903, 1507924, 1582356, 1628308,
  1628302, 1477958, 1628271, 1628179, 1559326, 1635072, 1540323, 1627993, 1498543, 1628428, 1501369, 1628587, 1628115, 1635162, 1628120, 1487044,
  1478225, 1635133, 1628162, 1602965, 1483539, 1628282, 1508036, 1471683, 1628067, 1590607, 1628567, 1507300, 1484103, 1581726, 1628429, 1529787,
  1530049, 1530050, 1531666, 1482125, 1628298, 1628449, 1470163, 1628451, 1495965, 1544851, 1628001, 1553311, 1628290, 1507427, 1549799, 1628168,
  1628775, 1509441, 1628650, 1543499, 1628231, 1566603, 1628274, 1628558, 1545001, 1628482, 1634976, 1484048, 1501981, 1628309, 1635091, 1634965,
  1567236, 1628592, 1628569, 1554817, 1634966, 1606818, 1483005, 1628553, 1628978, 1504181, 1628720, 1581724, 1627992, 1628563, 1628300, 1628462,
  1547826, 1498477, 1499692, 1628712, 1628167, 1507161, 1628269, 1606817, 1628430, 1556451, 1628234, 1627995, 1580648, 1482124, 1531665, 1549812,
  1553084, 1628245, 1628159, 1570310, 1628527, 1628432, 1504182, 1628176, 1536342, 1510653, 1628564, 1550043, 1628295, 1628106, 1507827, 1628980,
  1628812, 1479009, 1628562, 1628246, 1482974, 1602297, 1508360, 1628230, 1546763, 1606509, 1628156, 1628299, 1628267, 1628161, 1471598, 1545142,
  1565598, 1628248, 1553319, 1543498, 1635420, 1628169, 1628232, 1541485, 1628566, 1549959, 1547166, 1469260, 1628501, 1628581, 1628600, 1628266,
  1628536, 1628692, 1628450, 1628691, 1628237, 1509442, 1635153, 1507412, 1483305, 1628251, 1498420, 1628551, 1628101, 1470704, 1628848, 1628270,
  1528952, 1635065, 1628297, 1628452, 1628105, 1478618, 1635268, 1582017, 1530800, 1628313, 1628217, 1628559, 1487302, 1581014, 1628557, 1542549,
  1564073, 1510952, 1555740, 1635338, 1547332, 1635340, 1635341, 1628291, 1628296, 1635339, 1628287, 1635566, 1628471, 1484369, 1628556, 1628710,
  1628817, 1628420, 1628252, 1628225, 1485725, 1507132, 1628522, 1507471, 1628107, 1584572, 1628824, 1635161, 1591975, 1544710, 1527010, 1628778,
  1628249, 1628785, 1514807, 1628584, 1628554, 1628422, 1532390, 1628532, 1628789, 1558166, 1557695, 1628862, 1635387, 1628533, 1628226, 1574663,
  1499693, 1628092, 1628646, 1628166, 1507828, 1507905, 1628109, 1628097, 1581725, 1504183, 1628222, 1542294, 1635431, 1568501, 1511319, 1550042,
  1628247);

------------------------------------------------------------------------------------------------------------------------------------------

/*Ticket 25662 - Migração gerou boleto no contrato errado comandos para correção. IMPORTANTE 
nesse caso não gerou nota fiscal por isso foi feito up apenas em duas tabelas: de fatura 
e movimento financeiro.
*/

/*1º Pelo nosso numero é possível buscar o num da fatura
*/

select * from docreceber dr

WHERE dr.nossonumero = 3631992

/*2º Com o numero da fatura é possível verificar o cód de contrato que precisará ser alterado (comando apenas 
para visualização)
*/

select * from movimfinanceiro mv

WHERE mv.numfatura = 34672001

/*3º Com o numero da fatura é possível verificar o cód de contrato que precisará ser alterado (comando apenas 
para visualização)
*/

select * from fatura ft

WHERE ft.numerofatura = 34672001


/*4º Para resolução comandos a baixo o que vai alterar automaticamente nas duas tabelas ao mesmo tempo.
Apenas por select não é possível fazer essa correção precisa ser ao mesmo tempo em todas as 
tabelas envolvidas
*/

update movimfinanceiro set contrato = 992186 where id in (        
  select mv.id
  from movimfinanceiro mv
  WHERE mv.numfatura = 34672001
);

update fatura set numerodocontrato = 992186 where id in (
  select ft.id
  from fatura ft
  WHERE ft.numerofatura = 34672001
);

------------------------------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT b."REGIONAL",
b."CIDADE",
b."ID CONTRATO",
b."CONTRATO",
b."ID HISTORICO"[1] AS "ID HISTORICO",
b."DATA ATENDIMENTO"[1] AS "DATA ATENDIMENTO",
b."HORA ATENDIMENTO"[1] AS "HORA ATENDIMENTO",
b."ATENDENTE"[1] AS "ATENDENTE",
b."ASSUNTO",
b."DESCRICAO ATENDIMENTO"[1] AS "DESCRICAO ATENDIMENTO",
b."VALOR ADITIVO"[1] AS "VALOR ADITIVO"
FROM ( SELECT DISTINCT a."REGIONAL",
a."CIDADE",
a."ID CONTRATO",
a."CONTRATO",
array_agg(a."ID HISTORICO"[1] ORDER BY a."ID HISTORICO" DESC) AS "ID HISTORICO",
array_agg(a."DATA ATENDIMENTO"[1] ORDER BY a."ID HISTORICO" DESC) AS "DATA ATENDIMENTO",
array_agg(a."HORA ATENDIMENTO"[1] ORDER BY a."ID HISTORICO" DESC) AS "HORA ATENDIMENTO",
array_agg(a."ATENDENTE"[1] ORDER BY a."ID HISTORICO" DESC) AS "ATENDENTE",
a."ASSUNTO",
array_agg(a."DESCRICAO ATENDIMENTO"[1] ORDER BY a."ID HISTORICO" DESC) AS "DESCRICAO ATENDIMENTO",
array_agg(a."Valor" ORDER BY a."ID HISTORICO" DESC) AS "VALOR ADITIVO"
FROM ( SELECT r.descricao AS "REGIONAL",
cid.nomedacidade AS "CIDADE",
ct.id AS "ID CONTRATO",
ct.contrato AS "CONTRATO",
array_agg(hg.id) AS "ID HISTORICO",
array_agg(hg.d_data) AS "DATA ATENDIMENTO",
array_agg(date_trunc('second'::text, hg.t_hora::interval)::text) AS "HORA ATENDIMENTO",
array_agg(hg.atendente) AS "ATENDENTE",
gh.descricao AS "ASSUNTO",
array_agg(ah.descricao) AS "DESCRICAO ATENDIMENTO",

sum(func_calculavaloraditivos_v2(ct.cidade, ct.codempresa, ct.contrato, pg.tipoponto, pg.tipoprogramacao, cp.valorpacote, date_trunc('month',hg.d_data - interval '1 month')::date, (date_trunc('month',(hg.d_data))-interval '1 day')::date, cp.protabelaprecos))
as "Valor"

FROM historicogeral hg
JOIN assuntohistorico ah ON hg.grupoassunto = ah.codigogrupo AND hg.assunto = ah.codigoassunto
JOIN grupohistorico gh ON hg.grupoassunto = gh.codigo
JOIN contratos ct ON ct.cidade = hg.codigocidade AND ct.contrato = hg.codcontrato
JOIN cidade cid ON cid.codigodacidade = ct.cidade
JOIN regional r ON r.codigo = cid.codigo_regional
JOIN cont_prog cp ON cp.cidade = ct.cidade AND cp.contrato = ct.contrato
LEFT JOIN programacao pg ON pg.codigodaprogramacao = cp.protabelaprecos AND pg.codcidade = cp.cidade

-- ########## ALTERAR A DATA AQUI ###########

WHERE hg.d_data >= '01-03-2022' and hg.d_data <= '31-03-2022'

AND hg.historicopai IS NULL AND (hg.grupoassunto = ANY (ARRAY[851, 881]))
GROUP BY r.descricao, cid.nomedacidade, ct.id, ct.contrato, hg.id, gh.descricao, hg.grupoassunto, hg.historicopai) a
GROUP BY a."REGIONAL", a."CIDADE", a."ID CONTRATO", a."CONTRATO", a."ASSUNTO") b;

------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.estoque_movimentacao_periodo (
  pdatainicial date,
  pdatafinal date
)
RETURNS TABLE (
  datamovimentacao date,
  tipomovimentacao text,
  numeromiventacao bigint,
  armazemorigem text,
  armazemdestino text,
  codigoproduto bigint,
  descricaoproduto text,
  nomeservico text,
  ordemservico bigint,
  contrato bigint,
  quantidade integer
) AS
$body$
begin
  return query
  select mv.d_datacadastro::date,
    case
      when mv.codigopedido is not null then 'Pedido'::text
      when mv.codigorequisicao is not null then 'Requisição - Entrada'::text
      when position('REQ'::text in mv.idorigem::text) > 0 then 'Requisição - Saída'::text
      when mv.idmateriaisos is not null then 'Materiais Utilizados - Entrada'::text
      when position('MTU'::text in mv.idorigem::text) > 0 then 'Materiais Utilizados - Saída'::text
      when mv.idmateriaisretirados is not null then 'Materiais Retirados - Entrada'::text
      when position('MTR'::text in mv.idorigem::text) > 0 then 'Materiais Retirados - Saída'::text
      when mv.codtransferencia is not null then 'Transferência - Entrada'::text
      when position('TRF'::text in mv.idorigem::text) > 0 then 'Transferência - Saída'::text
      when mv.coddevolucao is not null then 'Devolução - Entrada'::text
      when position('DEV'::text in mv.idorigem::text) > 0 then 'Devolução - Saída'::text
    end,
    mv.codigo::bigint, a1.descricao::text, a.descricao::text, mv.codigoproduto::bigint, prd.descricao::text,
    case when l.id is not null then l.descricaodoserv_lanc::text else l1.descricaodoserv_lanc::text end,
    case when os.id is not null then os.numos::bigint else os1.numos::bigint end,
    case when os.id is not null then os.codigocontrato::bigint else os1.codigocontrato::bigint end,
    mv.quantidade::integer
  from public.movimentacaoproduto mv
  join public.produtos prd on prd.codigo = mv.codigoproduto
  join public.armazem a on a.codigo = mv.codarmazem
  left join public.movimentacaoproduto mv1 on case
    when mv.idorigem is not null then mv1.id = btrim(substr(mv.idorigem,4,20))::bigint
    else btrim(substr(mv1.idorigem,4,20))::bigint = mv.id end
  left join public.armazem a1 on a1.codigo = mv1.codarmazem
  left join public.materiaisos mtu on mtu.id = mv.idmateriaisos
  left join public.materiaisosretirada mtr on mtr.id = mv.idmateriaisretirados
  left join public.ordemservico os on os.cidade = mtu.codigocidade and os.codempresa = mtu.codempresa and os.numos = mtu.numos
  left join public.ordemservico os1 on os1.cidade = mtr.codigocidade and os1.codempresa = mtr.codempresa and os1.numos = mtr.numos
  left join public.lanceservicos l on l.codigodoserv_lanc = os.codservsolicitado
  left join public.lanceservicos l1 on l1.codigodoserv_lanc = os1.codservsolicitado
  where mv.d_datacadastro between pDataInicial and pDataFinal
  order by mv.id;
end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100 ROWS 1000;

ALTER FUNCTION public.estoque_movimentacao_periodo (pdatainicial date, pdatafinal date)
  OWNER TO hilton;

------------------------------------------------------------------------------------------------------------------------------------------

select ct.codigodocliente,
nome,
plano,
valor,
datavencimento,
datapgto,
contacredito

FROM contratos ct

------------------------------------------------------------------------------------------------------------------------------------------

select dr.codigodacidade,
       dr.cliente,
       cl.nome,
       array_agg(split_part(m.observacao, '[', 1)) as "observacaoo",
       dr.valordocumento,
       dr.d_datavencimento,
       dr.d_datafaturamento,
       dr.d_datapagamento,
       ct.codcontacredito
     LEFT JOIN docreceber dr on dr.cliente = ct.codigodocliente and dr.codigodacidade = ct.cidade
FROM contratos ct
     JOIN clientes cl ON cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente
     JOIN contascreditocidade cc ON cc.codigocidade = dr.codigodacidade and cc.codigoconta = ct.codcontacredito
     JOIN movimfinanceiro m on m.numfatura = dr.fatura
WHERE dr.d_datafaturamento IS NOT NULL and
      dr.d_datafaturamento BETWEEN '2022-05-01' and '2022-05-31' and
      ct.codcontacredito <> '41'
GROUP BY dr.codigodacidade,
         dr.cliente,
         cl.nome,
         dr.valordocumento,
         dr.d_datavencimento,
         dr.d_datafaturamento,
         dr.d_datapagamento,
         ct.codcontacredito
ORDER BY cl.nome

------------------------------------------------------------------------------------------------------------------------------------------

--ASSINANTES COM VALOR ZERADO
select distinct
	   c.nomedacidade as "Nome_Assinante",
       cl.codigocliente as "Cod_Assinante",
       cl.nome as "Nome_Assinante", 
       ct.contrato as "Contrato",
case
          when ct.situacao = 1 then 'Aguardando Conexão'
          when ct.situacao = 2 then 'Conectado'
          when ct.situacao = 3 then 'Pausado'
          when ct.situacao = 4 then 'Inadimplente'
          when ct.situacao = 5 then 'Cancelado'
          when ct.situacao = 6 then 'Endereço não Cabeado'
          when ct.situacao = 7 then 'Conectado/Inadimplente'
end as "Situacao_Contrato",
       pr.codigodaprogramacao as "Cod_Programacao",
       pr.nomedaprogramacao as "Nome_Programacao",
case  
      when cp.situacao = 1 then 'Ligado'
      when cp.situacao = 2 then 'Desligado'
end as "Situacao_Pacote",
      p.nomedoponto as "Nome_Ponto",
      cp.valorpacote as "Valor"
from contratos ct
     join cidade c on c.codigodacidade = ct.cidade
     join clientes cl on cl.cidade = ct.cidade and cl.codigocliente = ct.codigodocliente
     join cont_prog cp on cp.cidade = ct.cidade and cp.codempresa = ct.codempresa and cp.contrato = ct.contrato
     join programacao pr on pr.codcidade = cp.cidade and pr.codigodaprogramacao = cp.protabelaprecos
     join pontos p on p.cidade = cp.cidade and p.codempresa = cp.codempresa and p.contrato = cp.contrato and p.numerodoponto = cp.codigodoponto
where ct.situacao <> 5 and cp.valorpacote = 0 
order by c.nomedacidade,
         cl.nome

--------------------------------------------------------------------------------------------------------------------------------------------

--ALTERAR PARÂMETROS DE BOLETO
select * from parametrochavevalor

--------------------------------------------------------------------------------------------------------------------------------------------

--CLIENTES SEM GERAÇÃO DE BOLETO
select c.nomedacidade,
       ct.codigodocliente,
       cl.nome,
       ct.contrato,
       CASE
         when ct.tipodocontrato = 221 then 'UNIMED - JAU'
         when ct.tipodocontrato = 231 then 'CONTRATO MINEIROS'
         when ct.tipodocontrato = 211 then 'PERMUTA'
         when ct.tipodocontrato = 271 then 'PERMUTA'
       end as tipodocontrato,
       case
         when ct.situacao = 1 then 'Aguardando Conexão'
         when ct.situacao = 2 then 'Conectado'
         when ct.situacao = 3 then 'Pausado'
         when ct.situacao = 4 then 'Inadimplente'
         when ct.situacao = 5 then 'Cancelado'
         when ct.situacao = 6 then 'Endereço não Cabeado'
         when ct.situacao = 7 then 'Conectado/Inadimplente'
       end as "Situacao_Contrato"
from contratos ct
     join cidade c on c.codigodacidade = ct.cidade
     join clientes cl on cl.cidade = ct.cidade and cl.codigocliente = ct.codigodocliente
where ct.tipodocontrato IN (221, 231, 211, 271) and
      ct.situacao <> 5

--------------------------------------------------------------------------------------------------------------------------------------------

select t.*
from (
       select codigopedido,
              codigoproduto,
              descricaoproduto,
              datamovimentacao,
              tipomovimentacao,
              armazemorigem,
              armazemdestino,
              numeromiventacao
       from regrasoperacao.estoque_movimentacao_periodo('2022-05-01'::date, '2022-05-01'::date, 27::smallint)
     ) as t

--------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW regrasoperacao.vis_relatorio_faturamento_contacredito(
    nomecidade,
    codigocliente,
    nomecliente,
    observacaoboleto,
    numerodocumento,
    valordocumento,
    datavencimento,
    datafaturamento,
    datapagamento,
    localdecobranca,
    descricao,
    codigocontacredito,
    descricaocontacredito)
AS
  SELECT c.nomedacidade AS nomecidade,
         dr.cliente AS codigocliente,
         cl.nome AS nomecliente,
         array_agg(split_part(m.observacao::text, '['::text, 1)) AS observacaoboleto,
         dr.numerodocumento,
         dr.valordocumento,
         dr.d_datavencimento AS datavencimento,
         dr.d_datafaturamento AS datafaturamento,
         dr.d_datapagamento AS datapagamento,
         dr.localcobranca AS localdecobranca,
         l.descricao,
         dr.codcontacredito AS codigocontacredito,
         cc.conta_descricao AS descricaocontacredito
  FROM contratos ct
       LEFT JOIN docreceber dr ON dr.cliente = ct.codigodocliente AND dr.codigodacidade = ct.cidade
       JOIN cidade c ON c.codigodacidade = dr.codigodacidade
       JOIN clientes cl ON cl.cidade = dr.codigodacidade AND cl.codigocliente = dr.cliente
       JOIN localcobranca l ON l.codigo = dr.localcobranca
       JOIN contascreditocidade cc ON cc.codigocidade = dr.codigodacidade AND cc.codigoconta = dr.codcontacredito
       JOIN movimfinanceiro m ON m.numfatura = dr.fatura
  WHERE dr.d_datafaturamento IS NOT NULL
  GROUP BY c.nomedacidade,
           dr.cliente,
           cl.nome,
           dr.valordocumento,
           dr.d_datavencimento,
           dr.d_datafaturamento,
           dr.d_datapagamento,
           dr.codcontacredito,
           dr.localcobranca,
           l.descricao,
           dr.numerodocumento,
           cc.conta_descricao
  ORDER BY c.nomedacidade,
           cl.nome;

--------------------------------------------------------------------------------------------------------------------------------------------

select c.nomedacidade,
       dr.cliente,
       cl.nome,
       array_agg(split_part(m.observacao, '[', 1)) as "observacao_boleto",
       dr.numerodocumento,
       dr.valordocumento,
       dr.d_datavencimento,
       dr.d_datafaturamento,
       dr.d_datapagamento,
       dr.localcobranca,
       l.descricao,
       dr.codcontacredito,
       cc.conta_descricao
FROM contratos ct
     LEFT JOIN docreceber dr on dr.cliente = ct.codigodocliente and dr.codigodacidade = ct.cidade
     join cidade c on c.codigodacidade = dr.codigodacidade
     JOIN clientes cl ON cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente
     join localcobranca l on l.codigo = dr.localcobranca
     JOIN contascreditocidade cc ON cc.codigocidade = dr.codigodacidade and cc.codigoconta = dr.codcontacredito
     JOIN movimfinanceiro m on m.numfatura = dr.fatura
WHERE dr.d_datafaturamento IS NOT NULL and
      dr.d_datafaturamento BETWEEN '2022-05-01' and '2022-05-31'
GROUP BY c.nomedacidade,
         dr.cliente,
         cl.nome,
         dr.valordocumento,
         dr.d_datavencimento,
         dr.d_datafaturamento,
         dr.d_datapagamento,
         dr.codcontacredito,
         dr.localcobranca,
         l.descricao,
         dr.numerodocumento,
         cc.conta_descricao
ORDER BY c.nomedacidade,
         cl.nome

--------------------------------------------------------------------------------------------------------------------------------------------

with 
	x as (SELECT ct_1.id, ls_1.descricaodoserv_lanc, count(os_1.id) as qtde, os_1.d_dataexecucao
	FROM ordemservico os_1
	JOIN lanceservicos ls_1 ON ls_1.codigodoserv_lanc = os_1.codservsolicitado
	JOIN contratos ct_1 ON ct_1.cidade = os_1.cidade AND ct_1.codempresa = os_1.codempresa AND ct_1.contrato = os_1.codigocontrato
	where os_1.codservsolicitado = 1271 
	group by ct_1.id, ls_1.descricaodoserv_lanc, d_dataexecucao ), 
    y as (SELECT ct_2.id, ls_2.descricaodoserv_lanc, count(os_2.id) as qtde, os_2.d_dataexecucao 
	FROM ordemservico os_2
	JOIN lanceservicos ls_2 ON ls_2.codigodoserv_lanc = os_2.codservsolicitado
	JOIN contratos ct_2 ON ct_2.cidade = os_2.cidade AND ct_2.codempresa = os_2.codempresa AND ct_2.contrato = os_2.codigocontrato
	where os_2.codservsolicitado = 1361 
	group by ct_2.id, ls_2.descricaodoserv_lanc, os_2.d_dataexecucao ) 
    select distinct cli.codigocliente, cli.nome, func_retornatelefones(ct.cidade, ct.codigodocliente) as telefones, x.qtde as "Quantidade de VERIFICAÇÃO DE IRREGULARIDADES- INTERNET",
    x.d_dataexecucao, y.qtde as "Quantidade de VERIFICAÇÃO NO LOCAL",  y.d_dataexecucao
from ordemservico os
JOIN lanceservicos l ON l.codigodoserv_lanc = os.codservsolicitado
JOIN cidade cid ON cid.codigodacidade = os.cidade
JOIN clientes cli ON cli.cidade = os.cidade AND cli.codigocliente = os.codigoassinante
JOIN contratos ct ON ct.cidade = os.cidade AND ct.codempresa = os.codempresa AND ct.contrato = os.codigocontrato
join x on x.id=ct.id
join y on y.id=ct.id

--------------------------------------------------------------------------------------------------------------------------------------------

 SELECT * FROM OPENQUERY

select * from 	acao.totvs_boleto
WHERE
tipotransacao='U'
--AND situacao = 0
--and idbase=2
and idboleto in(
10345193,
10225400,
10359559
        )
        
select * from auditoria.aud_historicogeral l

WHERE id = 70

select * from auditoria.aud_assuntohistorico a
 where a.codigoassunto = 31 and a.codigogrupo = 291      
     

     
 select * from auditoria.aud_historicogeral l
 where l.id = 47725449
      
      
 select * from historicogeral l
 where l.codigocidade = 329721 and l.assinante = 739631 
      
 select * from cidade
 
 
 select * from campanhadocreceber c
 where c.d_datacadastro = '2022-03-17'
 
select distinct a.id as "ID_assinatura", t.nome as "Cidade", a.nome as "Nome_cliente", a.cpfcnpj as "CPF_CNPJ", date(a.criadoem) as datacriacao,
date(a.dataprocessamento) as "Data_processamento", caa.descricao as "Classificação_andamento",
btrim(s.descricao) as situacao,
an.nome as analista,
pt.vis_nome_pacote as "Pacote", pt.vis_valor as "Valor_pacote", ta.nomedatabeladeprecos as "Tabela_preco"
from interfocusprospect.assinatura a
join interfocusprospect.usuariolocal u on u.id=a.captador
join interfocusprospect.statusassinatura s on s.id=a.statusassinatura
join public. t on t.id=a.municipioterceirosconexao
left join interfocusprospect.usuariolocal an on an.id=a.analistaid
join public.vendedores v on v.id=u.vendedorterceiros
join public.canaisdevenda ca on ca.cidade=v.cidadeondetrabalha and ca.codigo=v.canalvenda
join interfocusprospect.assinaturapacoteterceiros ap on ap.assinatura=a.id
JOIN interfocusprospect.vis_pacotetabela pt ON pt.vis_id =ap.pacoteterceiros
join tabeladeprecos ta on ta.id=pt.vis_id_tabela_preco
join interfocusprospect.assinaturaandamento aa on aa.assinatura = a.id
join interfocusprospect.classificacaoandamento caa on caa.id = aa.classificacaoandamento
where date(a.criadoem) between '2022-01-01' and '2022-03-18'
order by t.nome, a.id

select distinct a.id as "ID_assinatura", t.nome as "Cidade", a.nome as "Nome_cliente", a.cpfcnpj as "CPF_CNPJ", date(a.criadoem) as datacriacao,
date(a.dataprocessamento) as "Data_processamento", caa.descricao as "Classificação_andamento",
btrim(s.descricao) as situacao, u.nome as "vendedor",
an.nome as analista,
pt.vis_nome_pacote as "Pacote", pt.vis_valor as "Valor_pacote", ta.nomedatabeladeprecos as "Tabela_preco"
from interfocusprospect.assinatura a
join interfocusprospect.usuariolocal u on u.id=a.captador
join interfocusprospect.statusassinatura s on s.id=a.statusassinatura
join public.tablocal t on t.id=a.municipioterceirosconexao
left join interfocusprospect.usuariolocal an on an.id=a.analistaid
join public.vendedores v on v.id=u.vendedorterceiros
join public.canaisdevenda ca on ca.cidade=v.cidadeondetrabalha and ca.codigo=v.canalvenda
join interfocusprospect.assinaturapacoteterceiros ap on ap.assinatura=a.id
JOIN interfocusprospect.vis_pacotetabela pt ON pt.vis_id =ap.pacoteterceiros
join tabeladeprecos ta on ta.id=pt.vis_id_tabela_preco
join interfocusprospect.assinaturaandamento aa on aa.assinatura = a.id
join interfocusprospect.classificacaoandamento caa on caa.id = aa.classificacaoandamento
where date(a.criadoem) between '2022-03-01' and '2022-03-22'
order by t.nome, a.id

--------------------------------------------------------------------------------------------------------------------------------------------

Select c.nomedacidade as "Cidade",
       cl.codigocliente as "Cod_Assinante",
       cl.nome as "Nome_Assinante",
       dr.numerodocumento as "Numero_Documento",
       dr.d_datavencimento as "Data_Vencimento",
       dr.nossonumero as "Nosso_Numero",
       cc.codigoconta as "Conta_Crédito",
       b.nome as "Banco"
from docreceber dr
     join public.cidade c on c.codigodacidade = dr.codigodacidade
     join public.clientes cl on cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente
     join public.contascreditocidade cc on cc.codigocidade = dr.codigodacidade and cc.codigoconta = dr.codcontacredito
     join public.bancos b on b.numero = cc.banco
where dr.d_datavencimento = '2022-04-04' and  dr.nossonumero in (10752972, 10774412, 10823572, 10844942, 10856572,
  10857222, 10864442, 10869642, 10874612, 10877132, 10893042, 10899792,
  10903542, 10907702, 12061322, 10988052, 10991702, 11005922, 11020692,
  11066782, 11114342, 11086772, 11099432, 11101972, 11103872, 11108802,
  11108902, 11111362, 11176652, 11189152, 11272752, 11221402, 11506702,
  11225192, 11192132, 11224902, 11195232, 11261572, 11320152, 11219252,
  11324802, 11285172, 11329602, 11288312, 11289152, 11208722, 11334622,
  11254932, 11410902, 11307322, 11357582, 11465772, 11371772, 11375202,
  11375862, 11383472, 11390672, 11401242, 11401332, 11411322, 11411902,
  11413082, 11414832, 11417002, 11417142, 11417452, 11417952, 11419822,
  11420042, 11422152, 11422412, 11422672, 11426172, 11426332, 11426552,
  11430382, 11430542, 11432512, 11433332, 11433702, 11435322, 11435812,
  11442782, 11443952, 11446652, 11449952, 11455912, 11462722, 11476742,
  11466362, 11466812, 11473182, 11477522, 11482912, 11486692, 11487302,
  11487592, 11488862, 11489662, 11494032, 11495052, 11496432, 11504272,
  11506102, 11506262, 11509182, 11509212, 11586302, 11614982, 11622122,
  11618972, 11620982, 11627142, 11628602, 11636892, 11706472, 11714032,
  11747862, 11768252, 11768282, 11808262, 11812592, 11828062, 11830132,
  11944472, 11926432, 11934722, 11951942, 11952192, 11953052, 11954462,
  11995142, 11996512)
  order by c.nomedacidade

--------------------------------------------------------------------------------------------------------------------------------------------

select DISTINCT
       cl.codigocliente,
       ct.contrato,
       cl.nome, 
       array_to_string(ARRAY
       ( SELECT * FROM public.func_separaemail(public.valida_email(cl.email)) ), ','::text) as email,
       case
       when length(cl.cpf_cnpj) > 14 then 'JURIDICO'
       ELSE 'FISICO'
       end as tipopessoa
      
from public.clientes cl
     join public.telefones t on t.cidade = cl.cidade and t.codigocliente = cl.codigocliente
     left join public.contratos ct on ct.cidade = cl.cidade and ct.codigodocliente = cl.codigocliente
where public.valida_email(cl.email) is not null and
      ct.situacao in (2)

--------------------------------------------------------------------------------------------------------------------------------------------

--SVA MEGABIT--
SELECT ns.d_dataemissao as "data emissao",
ns.tiponf as "tipo nf",
ns.nomedacidade as "cidade",
ns.codigocliente as "cod. cliente",
ns.nome as "nome cliente",
ns.cpf_cnpj as "CPF_CNPJ",
ns.numnf as "numero nf",
ns.serienf as "serie nf",
ns.periodo as "periodo",
ns.totalnota as "total nota",
ns.baseicms as "base icms",
ns.valoricms as "valor icms",
ns.valorpis as "valor pis",
ns.valorcofins as "valor cofins",
ns.valorfust as "valor fust",
ns.valorfuntel as "valor funtel"
from regrasoperacao.vis_notas_sva ns
where ns.d_dataemissao between '2022-05-01' and '2022-05-31'

--------------------------------------------------------------------------------------------------------------------------------------------

select * from temporarias.func_executa_baixa_automatica_marretada()

--------------------------------------------------------------------------------------------------------------------------------------------

FUNÇÃOLIBERA ACESSO

SELECT * 
FROM func_libera_acesso('group_imanager')

------------------------------------------------------------------------------------------------------------------------------------------

with 
	x as ( SELECT cliente, nomecliente, numerodocumento, d_dataemissao, d_datavencimento, valordocumento, situacao, valordesconto, valorjuros, valormulta, valorpago, nossonumero
			FROM docreceber )

SELECT x.cliente, nome, x.numerodocumento, x.d_dataemissao, x.d_datavencimento, x.valordocumento, x.situacao, x.valordesconto, x.valorjuros, x.valormulta, x.valorpago, x.nossonumero

from clientes


JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
join cidade cid on cid.codigodacidade=cli.cidade
join x on x.id = ct.id

WHERE macaddress IS NOT NULL

------------------------------------------------------------------------------------------------------------------------------------------

CONECTA FIBRA

SELECT cliente, numerodocumento, d_dataemissao, d_datavencimento, valordocumento, situacao, valordesconto, valorjuros, valormulta, valorpago, nossonumero

FROM docreceber

limit 10

SELECT *
FROM clientes
limit 10

cliente, numerodocumento, d_dataemissao, d_datavencimento, valordocumento, situacao, valordesconto, valorjuros, valormmulta, valorpago, nossonumero
where

------------------------------------------------------------------------------------------------------------------------------------------

select distinct * from (
select DISTINCT cid.nomedacidade, cli.codigocliente, cli.nome,ct.contrato,ct.id,
CASE
	WHEN ct.situacao = 1 then 'Aguard. Conexão'::text
    WHEN ct.situacao = 2 then 'ConectadoAtivo'::text
    WHEN ct.situacao = 3 then 'Pausado'::text
    WHEN ct.situacao = 4 then 'Inadimplente'::text
    WHEN ct.situacao = 5 then 'Cancelado'::text
    WHEN ct.situacao = 6 then 'EndereçoNaoCabeado'::text 
    WHEN ct.situacao = 7 then 'ConectadoInadimplente'::text
END AS situacaocontrato,
x.tipoequipamento,
x.dataretirada,
x.id as "id.equipamento",
x.macaddress,
x.chassis,
x.slot,
x.porta,
x.serial,
x.datadesativacaomac
from cont_prog cp
left join cidade cid on cp.cidade = cid.codigodacidade
left join contratos ct on ct.cidade = cp.cidade and ct.contrato = cp.contrato
left join clientes cli on cli.cidade = cp.cidade and ct.codigodocliente = cli.codigocliente
left JOIN 
       (
         SELECT i.idcliente,
                i.tipoequipamento,
                i.dataretirada,
                i.id,
                i.macaddress,
                i.chassis,
                i.slot,
                i.porta,
                i.serial,
                i.datadesativacaomac
                
         FROM dblink(
           'hostaddr=187.63.192.133 port=5432 user=postgres password=i745@postgres dbname=ins'
           ::text, '
          select distinct  eq.idcliente,eq.macaddress,c.descricao as desc_chassi, ol.descricao as desc_slot,eq.portaoltchassis,
          case 
          when eq.tipoequipamento  = 9 then ''ONU''
          when eq.tipoequipamento = 7 then ''CPE RADIUS''
          end as tipoeq,
          eq.d_dataretirada,eq.id, eq.serial, eq.d_datadesativacaomac
          from idhcp.equipamentos eq
          left join idhcp.oltslot ol on ol.id = eq.idoltslot
          left join idhcp.oltchassis c on c.id = ol.idoltchassis 
          group by eq.idcliente,eq.macaddress,ol.descricao,c.descricao,eq.portaoltchassis,eq.ipconcentrador,eq.tipoequipamento,eq.d_dataretirada, eq.id
        '::text) i(idcliente bigint, macaddress text, chassis text, slot text, porta integer, tipoequipamento text,dataretirada date, id integer, serial text, datadesativacaomac date )
       ) x ON x.idcliente = ct.id
  where  x.id is not null
  
   union
       
       select DISTINCT cid.nomedacidade, cli.codigocliente, cli.nome,ct.contrato,ct.id,
CASE
	WHEN ct.situacao = 1 then 'Aguard. Conexão'::text
    WHEN ct.situacao = 2 then 'ConectadoAtivo'::text
    WHEN ct.situacao = 3 then 'Pausado'::text
    WHEN ct.situacao = 4 then 'Inadimplente'::text
    WHEN ct.situacao = 5 then 'Cancelado'::text
    WHEN ct.situacao = 6 then 'EndereçoNaoCabeado'::text 
    WHEN ct.situacao = 7 then 'ConectadoInadimplente'::text
END AS situacaocontrato,
x.tipoequipamento,
x.dataretirada,
x.id as "id.equipamento",
x.macaddress,
x.chassis,
x.slot,
x.porta,
x.serial,
x.datadesativacaomac
from cont_prog cp
left join cidade cid on cp.cidade = cid.codigodacidade
left join contratos ct on ct.cidade = cp.cidade and ct.contrato = cp.contrato
left join clientes cli on cli.cidade = cp.cidade and ct.codigodocliente = cli.codigocliente
left JOIN 
       (
          SELECT i.idcliente,
                i.tipoequipamento,
                i.dataretirada,
                i.id,
                i.macaddress,
                i.chassis,
                i.slot,
                i.porta,
                i.serial,
                i.datadesativacaomac
                
         FROM dblink(
           'hostaddr=177.129.48.5 port=5432 user=postgres password=i745@postgres dbname=ins'
           ::text, '
          select distinct  eq.idcliente,eq.macaddress,c.descricao as desc_chassi, ol.descricao as desc_slot,eq.portaoltchassis,
          case 
          when eq.tipoequipamento  = 9 then ''ONU''
          when eq.tipoequipamento  = 7 then ''CPE RADIUS''
          end as tipoeq,
          eq.d_dataretirada,eq.id, eq.serial, eq.d_datadesativacaomac
          from idhcp.equipamentos eq
          left join idhcp.oltslot ol on ol.id = eq.idoltslot
          left join idhcp.oltchassis c on c.id = ol.idoltchassis 
          group by eq.idcliente,eq.macaddress,ol.descricao,c.descricao,eq.portaoltchassis,eq.ipconcentrador,eq.tipoequipamento,eq.d_dataretirada, eq.id
        '::text) i(idcliente bigint, macaddress text, chassis text, slot text, porta integer, tipoequipamento text,dataretirada date, id integer, serial text, datadesativacaomac date )
       ) x ON x.idcliente = ct.id
  where  x.id is not null) as x
  limit 100

------------------------------------------------------------------------------------------------------------------------------------------

--UPDATE troca conta credito contrato
update contratos ct set codcontacredito = 91 where id in (
select ct.id
from contratos ct
join clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
join contascreditocidade cc on cc.codigocidade=ct.cidade and cc.codigoconta=ct.codcontacredito
where cc.codigocidade = 889601 and ct.situacao <> 5 );  --6755 assinante

update clientes cli set codcontacredito = 91 where id in (
select cli.id
from contratos ct
join clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
join contascreditocidade cc on cc.codigocidade=ct.cidade and cc.codigoconta=ct.codcontacredito
where cc.codigocidade = 889601 and ct.situacao <> 5 )

------------------------------------------------------------------------------------------------------------------------------------------

SELECT 	 cid . nomedacidade,
         pr.codigodaprogramacao,
         pr.nomedaprogramacao,
         cp.valorpacote,
         count(*) AS qtde,
         case
         	when ct.tipodocontrato = 11 then 'PADRAO'
            when ct.tipodocontrato = 21 then 'CEMIG'
            when ct.tipodocontrato = 31 then 'PM'
            when ct.tipodocontrato = 41 then 'SCTV'
            when ct.tipodocontrato = 61 then 'SCTV IRMAO'
            when ct.tipodocontrato = 71 then 'PERM'
            when ct.tipodocontrato = 81 then 'FARM.CENTRAL'
            when ct.tipodocontrato = 91 then 'SUPER CANAL'
            when ct.tipodocontrato = 101 then 'PCMG'
            when ct.tipodocontrato = 111 then 'PREFEITURA CTGA'
            when ct.tipodocontrato = 121 then 'TV SISTEC'
            when ct.tipodocontrato = 131 then 'CIVIL'
            when ct.tipodocontrato = 141 then 'PUBLICIDADE'
            when ct.tipodocontrato = 151 then 'PERMUTA'
            when ct.tipodocontrato = 161 then 'CORTESIA'
            when ct.tipodocontrato = 171 then 'PREFEITURAS'
            when ct.tipodocontrato = 181 then 'LOJAS BREDER'
            when ct.tipodocontrato = 191 then 'CORP.SCTV'
            when ct.tipodocontrato = 201 then 'MIGRAÇÃO'
            when ct.tipodocontrato = 211 then 'CX ESCOLAR'
            when ct.tipodocontrato = 221 then 'DPC'
         end as "tipo_contrato",
         
         CASE
	WHEN ct.situacao = 1 then 'Aguard. Conexão'::text
    WHEN ct.situacao = 2 then 'ConectadoAtivo'::text
    WHEN ct.situacao = 3 then 'Pausado'::text
    WHEN ct.situacao = 4 then 'Inadimplente'::text
    WHEN ct.situacao = 5 then 'Cancelado'::text
    WHEN ct.situacao = 6 then 'EndereçoNaoCabeado'::text 
    WHEN ct.situacao = 7 then 'ConectadoInadimplente'::text
END AS situacaocontrato
         
  FROM cont_prog cp
       JOIN contratos ct ON ct.cidade = cp.cidade AND ct.codempresa =
         cp.codempresa AND ct.contrato = cp.contrato
       JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente =
         ct.codigodocliente
       JOIN cidade cid ON cid . codigodacidade = ct.cidade
       JOIN programacao pr ON pr.codcidade = cp.cidade AND
         pr.codigodaprogramacao = cp.protabelaprecos

  GROUP BY cid . nomedacidade,
           pr.codigodaprogramacao,
           pr.nomedaprogramacao,
           cp.valorpacote,
           ct.tipodocontrato,
           ct.situacao

  ORDER BY cid . nomedacidade,
           pr.codigodaprogramacao,
           pr.nomedaprogramacao,
           cp.valorpacote

------------------------------------------------------------------------------------------------------------------------------------------

with 
	x as ( SELECT id, idcidade, nomecliente, username, macaddress
			FROM ins.equipamentos )

SELECT cid.nomedacidade, x.nomecliente, x.username, x.macaddress
from contratos ct


JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
join cidade cid on cid.codigodacidade=cli.cidade
join x on x.id = ct.id

WHERE macaddress IS NOT NULL

------------------------------------------------------------------------------------------------------------------------------------------

with 
	x as ( SELECT cliente, numerodocumento, d_dataemissao, d_datavencimento, valordocumento, situacao, valordesconto, valorjuros, valormulta, valorpago, nossonumero
			FROM docreceber )

SELECT x.cliente, cid.nome, x.numerodocumento, x.d_dataemissao, x.d_datavencimento, x.valordocumento, x.situacao, x.valordesconto, x.valorjuros, x.valormulta, x.valorpago, x.nossonumero
from clientes ct

JOIN clientes cli ON cli.nome = ct.nome
join x on x.id = ct.id

------------------------------------------------------------------------------------------------------------------------------------------

-- DESCONTOS PROGRAMACAO -- Situação 1 Simulação /  Situação 2 Gerar desconto
select * from temporarias.inclui_desconto_horas_tiraprog()

--Realizar desconto  Endereço -- Situação 1 Simulação /  Situação 2 Gerar desconto
 select * from temporarias.inclui_desconto_horas_endereco2()

------------------------------------------------------------------------------------------------------------------------------------------

select cli.id, cli.nome, ct.contrato
from contratos ct
join clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
join contascreditocidade cc on cc.codigocidade=ct.cidade and cc.codigoconta=ct.codcontacredito
where cc.codigocidade = 889601 and ct.situacao <> 5 


select cli.id, cli.nome, ct.contrato
from contratos ct
join clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
join contascreditocidade cc on cc.codigocidade=ct.cidade and cc.codigoconta=ct.codcontacredito
where cc.codigocidade = 889601 and ct.situacao <> 5 --6755 assinantes

------------------------------------------------------------------------------------------------------------------------------------------

--CÓDIGO PAGAMENTOS INVERTIDOS CONECTA FIBRA (OSWALDO CRUZ)
with x as(
         select dr.codigodacidade,
        		dr.cliente,
                cl.nome
	from docreceber dr
    		  join public.clientes cl on cl.cidade = dr.codigodacidade and
                cl.codigocliente = dr.cliente
		where dr.d_datavencimento BETWEEN '2022-05-01' and '2022-05-31' and 
                dr.d_datapagamento is not null and
                dr.situacao = 0),
               
 y as ( 
 select dr.codigodacidade,
        dr.cliente
 	from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
                 cl.codigocliente = dr.cliente
 		where dr.d_datavencimento <= '2022-04-30' and 
                 dr.d_datapagamento is  null and
                 dr.situacao = 0)
                            
 select x.* from x
 join y on y.codigodacidade = x.codigodacidade and y.cliente = x.cliente

------------------------------------------------------------------------------------------------------------------------------------------

--GERAR DESCONTO NODES
select * from temporarias.inclui_desconto_horas_node 
('331,261,271,281,351,361',
1160,
'2022-05-01'::date,
'',
'INDISPONIBILIDADE DE TV E INTERNET - 01/05/2022',
1)


------------------------------------------------------------------------------------------------------------------------------------------

->Encerr antigo
Olá! Como vai?

Informo que devido a falta de interação estarei encerrando este ticket.
Porém, caso haja necessidade de interagir novamente, peço para que abra um novo chamado!
Desde já agradecemos o contato, para quaisquer eventualidades estaremos à total disposição!

Teremos o maior prazer em lhe auxiliar! =)

------------------------------------------------------------------------------------------------------------------------------------------
--VIEW ERRADA
/*CREATE VIEW regrasoperacao.vis_pagamentos_invertidos ( 
	codigocidade,
    cliente,
    nome,
    datavencimento)
     
AS
 
SELECT with x as(
         select dr.codigodacidade,
                dr.cliente,
                cl.nome,
                dr.d_datavencimento
from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
                cl.codigocliente = dr.cliente
where          dr.d_datapagamento is not null and
               dr.situacao = 0),
               
 y as (  
 select dr.codigodacidade,
        dr.cliente 
 from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
                cl.codigocliente = dr.cliente
 where dr.d_datavencimento <= current_date - interval '1 month'  and 
               dr.d_datapagamento is  null and
               dr.situacao = 0)
                                              
 select x.* from x
 join y on y.codigodacidade = x.codigodacidade and y.cliente = x.cliente;*/

------------------------------------------------------------------------------------------------------------------------------------------
--VIEW ERRADA
/*
select 			dr.codigodacidade,
                dr.cliente,
                cl.nome,
                dr.d_datavencimento
from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
                cl.codigocliente = dr.cliente and dr.d_datapagamento is not null and dr.situacao = 0
      
left join 
( 
select dr.codigodacidade,
       dr.cliente,
       dr.d_datavencimento
from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
                cl.codigocliente = dr.cliente
where dr.d_datapagamento is null and dr.d_datavencimento <= current_date - interval '1 month' and
      dr.situacao = 0
) as x on x.codigodacidade = dr.codigodacidade and x.cliente = dr.cliente 
where dr.d_datavencimento BETWEEN '2022-05-01' and '2022-05-31' and
      x.d_datavencimento <= current_date*/

------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW regrasoperacao.vis_pagamentos_invertidos(
    codigocidade,
    cliente,
    nome,
    datavencimento)
AS
  SELECT dr.codigodacidade AS codigocidade,
         dr.cliente,
         cl.nome,
         dr.d_datavencimento AS datavencimento
  FROM docreceber dr
       JOIN clientes cl ON cl.cidade = dr.codigodacidade AND 
                     cl.codigocliente = dr.cliente AND 
                     dr.d_datapagamento IS NOT NULL AND 
                     dr.situacao = 0
       LEFT JOIN 
       (
         SELECT dr_1.codigodacidade,
                dr_1.cliente,
                dr_1.d_datavencimento,
         FROM docreceber dr_1
              JOIN clientes cl_1 ON cl_1.cidade = dr_1.codigodacidade AND 
                            cl_1.codigocliente = dr_1.cliente
         WHERE dr_1.d_datapagamento IS NULL AND
               dr_1.d_datavencimento <=(CURRENT_DATE - '1 mon'::interval) AND
               dr_1.situacao = 0
       ) x ON x.codigodacidade = dr.codigodacidade AND x.cliente = dr.cliente
  WHERE x.d_datavencimento <= CURRENT_DATE;

------------------------------------------------------------------------------------------------------------------------------------------

SELECT * FROM regrasoperacao.vis_pagamentos_invertidos

------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW regrasoperacao.vis_ordem_servico_abertas_tecnet(
    nomecidade,
    codigoassinante,
    idcliente,
    nome,
    tipocliente,
    idcontrato,
    codigocontrato,
    tipodologradouro,
    nomelogradouro,
    numeroconexao,
    bairroconexao,
    complementoconexao,
    numerodoformulario,
    nomevendedor,
    descricaosituacao,
    servico,
    data_atendimento,
    data_agendameanto,
    hora_agendamento,
    bairro,
    equipe,
    motivo_cancelamento,
    usuario_abriu,
    cpf_cnpj,
    macaddress,
    ipconcentrador,
    canal_venda)
AS
  SELECT cid . nomedacidade AS nomecidade,
         cli.id AS codigoassinante,
         ord.codigoassinante AS idcliente,
         cli.nome,
         CASE
           WHEN length(translate(cli.cpf_cnpj::text, '.-/'::text, ''::text)) > 11 THEN 'Pessoa Jurídica'::text
           ELSE 'Pessoa Física'::text
         END AS tipocliente,
         ct.id AS idcontrato,
         ord.codigocontrato,
         ed.tipodologradouro,
         ed.nomelogradouro,
         ct.numeroconexao,
         ct.bairroconexao,
         ct.complementoconexao,
         ct.numerodoformulario,
         vd.nome AS nomevendedor,
         v.descricaosituacao,
         l.descricaodoserv_lanc AS servico,
         ord.d_dataatendimento AS data_atendimento,
         ord.d_dataagendamento AS data_agendameanto,
         ord.t_horaatendimento AS hora_agendamento,
         ct.bairroconexao AS bairro,
         e.nomedaequipe AS equipe,
         m.descmotivo AS motivo_cancelamento,
         ord.atendente AS usuario_abriu,
         cli.cpf_cnpj,
         i.macaddress,
         i.ipconcentrador,
         tv.descricao AS canal_venda
  FROM ordemservico ord
       JOIN lanceservicos l ON l.codigodoserv_lanc = ord.codservsolicitado
       JOIN cidade cid ON cid . codigodacidade = ord.cidade
       JOIN clientes cli ON cli.cidade = ord.cidade AND cli.codigocliente = ord.codigoassinante
       JOIN contratos ct ON ct.cidade = ord.cidade AND ct.codempresa = ord.codempresa AND ct.contrato = ord.codigocontrato
       JOIN equipesdevenda eqv ON eqv.codigo = ct.equipedevenda AND eqv.cidade = ct.cidade
       JOIN vendedores vd ON vd.equipevenda = eqv.codigo AND vd.codigo = ct.vendedor AND vd.cidadeondetrabalha = cid . codigodacidade
       JOIN enderecos ed ON ct.enderecoconexao = ed.codigodologradouro AND cid . codigodacidade = ed.codigodacidade
       JOIN vis_situacaocontrato_descricao v ON v.situacao = ct.situacao
       LEFT JOIN equipe e ON e.codigocidade = ord.cidade AND e.codigodaequipe = ord.equipe
       LEFT JOIN motivocancelamento m ON m.codmotivo = ord.motivocancelamento
       LEFT JOIN tiposdevenda tv ON tv.codigo = ct.tipodevenda
       LEFT JOIN 
       (
         SELECT t.idcliente,
                t.macaddress,
                t.ipconcentrador
         FROM dblink('hostaddr=150.230.79.177 dbname=ins user=postgres password=i745@postgres port=5432'::text,
          'select equ.idcliente, equ.macaddress, equ.ipconcentrador
        from idhcp.equipamentos equ'::text) t(idcliente bigint, macaddress text, ipconcentrador text)
       ) i ON i.idcliente = ct.id
  WHERE cid . codigo_regional = 21 AND
        ord.situacao <> 3;


--FUNÇÃO



BEGIN
      Create temporary table temp_rp_tec_ordens_de_servico_abertas(
          "CIDADE" varchar(30),
          "CODIGO" integer,
          "NOME_CLIENTE" varchar(40),
          "CONTRATO" integer,
          "SITUAÇÃO" text,
          "TIPO" text,
          "ENDEREÇO" text,
          "NUMERO" varchar(10),
          "BAIRRO" varchar(20),
          "COMPLEMENTO" text,
          "FORMULARIO" text,
          "VENDEDOR" varchar(40),
          "SERVIÇO" varchar(40),
          "MOTIVOCANCELAMENTO" varchar(50),
          "ATENDIMENTO" text,
          "AGENDAMENTO" text,
          "HORA-AGENDAMENTO" text,
          "EQUIPE" varchar(30),
          "QUEM_ABRIU" text

    ) On commit drop;
       
        insert into temp_rp_tec_ordens_de_servico_abertas
          select distinct
            os.nomecidade as "Cidade",
            os.codigoassinante as "Código",
            os.nome as "Nome Cliente",
            os.codigocontrato as "Contrato",
            os.descricaosituacao as "Situação",
            os.tipodologradouro as "Tipo",
            os.nomelogradouro as "Endereço",
            os.numeroconexao as "Número",
            os.bairroconexao as "Bairro",
            CASE
                WHEN os.complementoconexao is NULL
                THEN 'SEM COMPLEMENTO' ELSE
                os.complementoconexao END AS "Complemento",
            os.numerodoformulario "Formulário",
            os.nomevendedor as "Vendedor",
            os.servico as "Serviço",
            os.motivo_cancelamento as "Motivo Cancelamento",
            to_char(os.data_atendimento, 'DD/MM/YYYY') as "Atendimento",
            to_char(os.data_agendameanto, 'DD/MM/YYYY') as "Agendamento",
            os.hora_agendamento as "HoraAgendamento",
            CASE
                WHEN os.equipe is null then 'SEM EQUIPE'
                ELSE os.equipe end as "Equipe",
            os.usuario_abriu as "Quem abriu"
          from regrasoperacao.vis_ordem_servico_abertas_tecnet os;
           
        return query select * from temp_rp_tec_ordens_de_servico_abertas;
        
    end;

------------------------------------------------------------------------------------------------------------------------------------------

CASE WHEN date_part('month', dr.d_datapagamento) < 10 
	THEN CONCAT(date_part('year', dr.d_datapagamento),'-0',date_part('month', dr.d_datapagamento))
ELSE CONCAT(date_part('year', dr.d_datapagamento),'-',date_part('month', dr.d_datapagamento)) END AS datapgto

------------------------------------------------------------------------------------------------------------------------------------------

select dr.codigodacidade,
       dr.cliente,
       cl.nome,
       dr.d_datavencimento
	from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
                	cl.codigocliente = dr.cliente and
               	 	dr.d_datapagamento is not null and 
               	 	dr.situacao = 0

left join 
( 
select dr.codigodacidade,
       dr.cliente,
       cl.nome,
       dr.d_datavencimento
	from docreceber dr
              join public.clientes cl on cl.cidade = dr.codigodacidade and
               	 	cl.codigocliente = dr.cliente

	where dr.d_datapagamento is null and
          dr.situacao = 0
) as x on x.codigodacidade = dr.codigodacidade and x.cliente = dr.cliente 

	where date_part('year', dr.d_datavencimento) = date_part('year', CURRENT_DATE) AND 
          date_part('month', dr.d_datavencimento) = date_part('month', CURRENT_DATE) AND
          x.d_datavencimento < TO_DATE(CONCAT('01/',date_part('month', CURRENT_DATE),'/',date_part('year', CURRENT_DATE)), 'DD-MM-YYYY') and
          dr.situacao = 0

------------------------------------------------------------------------------------------------------------------------------------------

-> 
Conforme solicitação finalizada, estaremos encerrando este ticket.


Porém, caso haja necessidade de interagir novamente, peço para que abra um novo chamado referenciando o mesmo!
Desde já agradecemos o contato, para quaisquer eventualidades estaremos à total disposição!


Teremos o maior prazer em lhe auxiliar! =)

------------------------------------------------------------------------------------------------------------------------------------------

--RELATÓRIO PERSONALIZADO (Pagamentos Invertidos) OK

--CABEÇALHO
Cidade;Código;Nome;Contrato;Data Pagamento;Data Vencimento Aberto

--SELECT
with 
  aberto as (   
     SELECT distinct dr_1.codigodacidade, dr_1.cliente, cl_1.nome, dr_1.d_datavencimento, m.contrato
     FROM docreceber dr_1
     join movimfinanceiro m on m.numfatura=dr_1.fatura
     JOIN clientes cl_1 ON cl_1.cidade = dr_1.codigodacidade AND cl_1.codigocliente = dr_1.cliente
     WHERE dr_1.d_datapagamento IS NULL AND dr_1.situacao = 0
  )
   select distinct cid.nomedacidade, cl.codigocliente, cl.nome, m.contrato, dr.d_datapagamento, a.d_datavencimento
   from docreceber dr
   join movimfinanceiro m on m.numfatura=dr.fatura
   JOIN clientes cl ON cl.cidade = dr.codigodacidade AND cl.codigocliente = dr.cliente
   join cidade cid on cid.codigodacidade=dr.codigodacidade
   join aberto a on a.codigodacidade=dr.codigodacidade and a.cliente=dr.cliente and a.contrato=m.contrato
   left join aberto aa on aa.codigodacidade=dr.codigodacidade and aa.cliente=dr.cliente and aa.contrato=m.contrato

--WHERE
where dr.d_datapagamento between pDataInicial and pDataFinal
   and a.d_datavencimento < pDataInicial
   and dr.d_datavencimento > aa.d_datavencimento

------------------------------------------------------------------------------------------------------------------------------------------

--ALTERAR INFORMAÇÕES/ENREDEÇO DE BOLETO CLIENTE
select * from central.conta

------------------------------------------------------------------------------------------------------------------------------------------

--PEGA DATA DO MES E ANO
to_char(current_date,'YYYY-MM-01')::date

------------------------------------------------------------------------------------------------------------------------------------------

SELECT rj.codigodacidade AS codigo_cidade,
         c.descricao AS carteira,
         rj.nomedacidade AS cidade_nome,
         rj.codigocliente AS cliente_codigo,
         rj.nome AS cliente_nome,
         rj.contrato AS contrato_codigo,
         rj.situacao_contrato AS contrato_situacao,
         rj.d_data::date AS data_reajuste,
         rj.codigodaprogramacao AS programacao_codigo,
         rj.nomedaprogramacao AS programacao_nome,
         rj.d_datadainstalacaocontrato AS contrato_instalacao,
         rj.d_dataativacaoprogramacao AS pacote_ativacao,
         rj.d_dataalttabelaprogramacao AS pacote_ultimo_reajuste,
         rj.valoratualpacote::numeric (15, 2) AS pacote_valor_atual,
         rj.valorpacotereajustado::numeric (15, 2) AS pacote_valor_reajustado,
         concat("substring"(rj.descricaoreajuste, 38, 5), '%') AS reajuste_indice,
         rj.contrato_id,
         rj.cont_prog_id AS pacote_id, 
         round(rj.valorpacotereajustado - rj.valoratualpacote,2) AS aumento,
         to_char(ct.d_datadainstalacao::timestamp with time zone, 'MM/YY'::text) AS periodo
  FROM reajustesefetivados rj
       JOIN contratos ct ON ct.id = rj.contrato_id
       JOIN carteira c ON c.codigo = ct.codcarteira

where rj.d_data BETWEEN '2021-05-01' and '2021-05-31'

------------------------------------------------------------------------------------------------------------------------------------------

--BOLETO MENORES QUE R$10
select c.nomedacidade as "Cidade",
cl.nome as "Nome_Assinante",
dr.numerodocumento as "Num_Documento",
dr.valordocumento as "Valor_Documento",
dr.d_datavencimento as "Data_Vencimento",
dr.id as "Id_Boleto"
from docreceber dr
join cidade c on c.codigodacidade = dr.codigodacidade
join public.clientes cl on cl.cidade = dr.codigodacidade and
cl.codigocliente = dr.cliente
where dr.valordocumento < 10.00 and
dr.situacao = 0 and
dr.formadepagamento = 1 and
dr.d_datapagamento is null
order by cl.nome

------------------------------------------------------------------------------------------------------------------------------------------

-- UPDATE PARA LIMPAR URL BOLETO
update docreceber set url_pdf_terceiros = NULL, arquivopdf = null
from (
  select dr.id, dr.d_datafaturamento, dr.url_pdf_terceiros
  from docreceber dr
  where dr.formadepagamento = 1 and dr.situacao = 0 and dr.d_datavencimento between '2022-06-01' and '2022-06-30' 
  and dr.d_datafaturamento = '2022-05-31' and dr.url_pdf_terceiros is not null
  AND dr.id = 2017393 --(APENAS PARA ESSE ID DE BOLETO) 
)
sql
where sql.id = docreceber.id

-- TRAZ BOLETO SEM URL
select dr.url_pdf_terceiros, * from docreceber dr where dr.d_datapagamento is null and dr.d_datavencimento > '2022-06-01'
and dr.url_pdf_terceiros is null

-- TRAZER ID BOLETO
select * from docreceber dr
where dr.nossonumero = 21476641

------------------------------------------------------------------------------------------------------------------------------------------

--MOVIMENTAÇÃO DE PRODUTO POR ARMAZÉM

--CABEÇALHOS
datamovimentacao;tipomovimentacao;numeromiventacao;armazemorigem;armazemdestino;codigoproduto;descricaoproduto;nomeservico;ordemservico;contrato;quantidade

select t.* from (
  select datamovimentacao, tipomovimentacao, numeromiventacao, armazemorigem, armazemdestino,
    codigoproduto, descricaoproduto, nomeservico, ordemservico, contrato, quantidade
  from intranet.estoque_movimentacao_periodo([dataInicio],[dataFim])
) as t

------------------------------------------------------------------------------------------------------------------------------------------

select distinct 
os.carteira as "Carteira", 
os.nomecidade as "Cidade", 
os.codigoassinante as "Código", 
os.nome as "Nome Cliente", 
os.codigocontrato as "Contrato",
os.descricaosituacao as "Situação", 
os.numos as "Nº OS", 
os.servico as "Serviço", 
os.data_atendimento as "Atendimento", to_char(os.horaatendimento,'HH24:MM') as "Hora Atendimento",
os.data_agendameanto as "Agendamento", 
os.data_execucao as "Execução", to_char(os.horaexecucao,'HH24:MM') as "Hora Execução",
os.data_realbaixa as "Data Real Baixa", 
os.bairro as "Bairro", 
os.equipe as "Equipe", 
os.equipeexecutou as "Equipe executou", 
os.tipo as "Tipo",
os.motivo_cancelamento as "Motivo Cancelamento", 
os.usuario_abriu as "Usuario que Abriu", 
os.vendedor as "Vendedor", 
os.pacote as "Pacote", 
os.valor_pacote as "Valor_Pacote",
os.endereco as "Endereço", 
os.numeroconexao as "Nº Conexão", 
os.aptoconexao as "Apto", 
os.blococonexao as "Bloco", 
os.bairroconexao as "Bairro", 
os.idcontrato as "Id Contrato",
os.ocorrencias as "Ocorrências"

from regrasoperacao.vis_ordem_servico_planos_conexao os

where os.carteira = 'WEBNET' AND OS.data_agendameanto is not null

------------------------------------------------------------------------------------------------------------------------------------------

select distinct 
       os.nomecidade,
       os.codigoassinante,
       os.nome,
       os.cpf_cnpj,
       os.codigocontrato,
       os.tipocontrato,
       os.descricaosituacao,
       os.numos,
       os.servico,
       os.data_atendimento,
       os.horaatendimento, 
       os.data_agendameanto,
       os.data_execucao,
       os.horaexecucao,
       os.horafinal, 
       os.data_realbaixa,
       os.equipe,
       os.equipeexecutou,
       os.tecnologia,
       os.tipo,
       os.motivo_cancelamento,
       os.usuario_abriu,
       os.grupo_usuario,
       os.vendedor,
       os.pacote,
       os.valor_pacote,
       os.valor_pacote_desconto,
       os.endereco,
       os.numeroconexao,
       os.aptoconexao,
       os.blococonexao,
       os.bairroconexao,
       os.idcontrato,
       os.ocorrencias,
       os.carteira,
       os.data_ativacao, 
       os.canal_venda, 
       os.equipe_venda
       from regrasoperacao.vis_ordem_servico_planos_conexao os
       
       
       where os.data_agendameanto is not null and os.carteira = 'WEBNET'

------------------------------------------------------------------------------------------------------------------------------------------

--UPDATE TIPOS DE CONTRATOS
update contratos
set tipodocontrato = 21
where contrato in (1628280, 1628277, 1628279, 1628278, 1628235, 1480000, 1628685, 1554735, 1554734, 
                   1564204, 1561469, 1564266, 1564169, 1557049, 1554732, 1554761, 1554760, 1554743, 
                   1562716, 1561488, 1628157, 1498694, 1566616, 1628283, 1628112, 1628096, 1635540, 
                   1483412, 1635105, 1628694,  1564110, 1504375, 1628116, 1628681, 1565680);


update contratos
set tipodocontrato = 71
where contrato in (1628102, 1468987, 1561400, 1628816, 1628483, 1628227, 1628709, 1628537, 1628423, 1538242, 1503351, 1528547, 1483369, 1628981,
  1483540, 1628281, 1628303, 1508037, 1628977, 1628301, 1628103, 1507210, 1628586, 1628286, 1628305, 1479724, 1628312, 1509982, 1541052, 1553011,
  1468692, 1628220, 1541484, 1628457, 1532382, 1628605, 1484064, 1628098, 1628573, 1560262, 1628517, 1628549, 1581271, 1628571, 1628714, 1628572,
  1628550, 1635505, 1504324, 1635073, 1541134, 1628250, 1545486, 1628711, 1504465, 1628814, 1628285, 1628607, 1628443, 1495844, 1628294, 1542896,
  1635152, 1553530, 1627990, 1628210, 1628257, 1628160, 1628158, 1628292, 1466933, 1628784, 1628100, 1505758, 1507903, 1507924, 1582356, 1628308,
  1628302, 1477958, 1628271, 1628179, 1559326, 1635072, 1540323, 1627993, 1498543, 1628428, 1501369, 1628587, 1628115, 1635162, 1628120, 1487044,
  1478225, 1635133, 1628162, 1602965, 1483539, 1628282, 1508036, 1471683, 1628067, 1590607, 1628567, 1507300, 1484103, 1581726, 1628429, 1529787,
  1530049, 1530050, 1531666, 1482125, 1628298, 1628449, 1470163, 1628451, 1495965, 1544851, 1628001, 1553311, 1628290, 1507427, 1549799, 1628168,
  1628775, 1509441, 1628650, 1543499, 1628231, 1566603, 1628274, 1628558, 1545001, 1628482, 1634976, 1484048, 1501981, 1628309, 1635091, 1634965,
  1567236, 1628592, 1628569, 1554817, 1634966, 1606818, 1483005, 1628553, 1628978, 1504181, 1628720, 1581724, 1627992, 1628563, 1628300, 1628462,
  1547826, 1498477, 1499692, 1628712, 1628167, 1507161, 1628269, 1606817, 1628430, 1556451, 1628234, 1627995, 1580648, 1482124, 1531665, 1549812,
  1553084, 1628245, 1628159, 1570310, 1628527, 1628432, 1504182, 1628176, 1536342, 1510653, 1628564, 1550043, 1628295, 1628106, 1507827, 1628980,
  1628812, 1479009, 1628562, 1628246, 1482974, 1602297, 1508360, 1628230, 1546763, 1606509, 1628156, 1628299, 1628267, 1628161, 1471598, 1545142,
  1565598, 1628248, 1553319, 1543498, 1635420, 1628169, 1628232, 1541485, 1628566, 1549959, 1547166, 1469260, 1628501, 1628581, 1628600, 1628266,
  1628536, 1628692, 1628450, 1628691, 1628237, 1509442, 1635153, 1507412, 1483305, 1628251, 1498420, 1628551, 1628101, 1470704, 1628848, 1628270,
  1528952, 1635065, 1628297, 1628452, 1628105, 1478618, 1635268, 1582017, 1530800, 1628313, 1628217, 1628559, 1487302, 1581014, 1628557, 1542549,
  1564073, 1510952, 1555740, 1635338, 1547332, 1635340, 1635341, 1628291, 1628296, 1635339, 1628287, 1635566, 1628471, 1484369, 1628556, 1628710,
  1628817, 1628420, 1628252, 1628225, 1485725, 1507132, 1628522, 1507471, 1628107, 1584572, 1628824, 1635161, 1591975, 1544710, 1527010, 1628778,
  1628249, 1628785, 1514807, 1628584, 1628554, 1628422, 1532390, 1628532, 1628789, 1558166, 1557695, 1628862, 1635387, 1628533, 1628226, 1574663,
  1499693, 1628092, 1628646, 1628166, 1507828, 1507905, 1628109, 1628097, 1581725, 1504183, 1628222, 1542294, 1635431, 1568501, 1511319, 1550042,
  1628247);

------------------------------------------------------------------------------------------------------------------------------------------

/*Ticket 25662 - Migração gerou boleto no contrato errado comandos para correção. IMPORTANTE 
nesse caso não gerou nota fiscal por isso foi feito up apenas em duas tabelas: de fatura 
e movimento financeiro.
*/

/*1º Pelo nosso numero é possível buscar o num da fatura
*/

select * from docreceber dr

WHERE dr.nossonumero = 3631992

/*2º Com o numero da fatura é possível verificar o cód de contrato que precisará ser alterado (comando apenas 
para visualização)
*/

select * from movimfinanceiro mv

WHERE mv.numfatura = 34672001

/*3º Com o numero da fatura é possível verificar o cód de contrato que precisará ser alterado (comando apenas 
para visualização)
*/

select * from fatura ft

WHERE ft.numerofatura = 34672001


/*4º Para resolução comandos a baixo o que vai alterar automaticamente nas duas tabelas ao mesmo tempo.
Apenas por select não é possível fazer essa correção precisa ser ao mesmo tempo em todas as 
tabelas envolvidas
*/

update movimfinanceiro set contrato = 992186 where id in (        
  select mv.id
  from movimfinanceiro mv
  WHERE mv.numfatura = 34672001
);

update fatura set numerodocontrato = 992186 where id in (
  select ft.id
  from fatura ft
  WHERE ft.numerofatura = 34672001
);

------------------------------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT b."REGIONAL",
b."CIDADE",
b."ID CONTRATO",
b."CONTRATO",
b."ID HISTORICO"[1] AS "ID HISTORICO",
b."DATA ATENDIMENTO"[1] AS "DATA ATENDIMENTO",
b."HORA ATENDIMENTO"[1] AS "HORA ATENDIMENTO",
b."ATENDENTE"[1] AS "ATENDENTE",
b."ASSUNTO",
b."DESCRICAO ATENDIMENTO"[1] AS "DESCRICAO ATENDIMENTO",
b."VALOR ADITIVO"[1] AS "VALOR ADITIVO"
FROM ( SELECT DISTINCT a."REGIONAL",
a."CIDADE",
a."ID CONTRATO",
a."CONTRATO",
array_agg(a."ID HISTORICO"[1] ORDER BY a."ID HISTORICO" DESC) AS "ID HISTORICO",
array_agg(a."DATA ATENDIMENTO"[1] ORDER BY a."ID HISTORICO" DESC) AS "DATA ATENDIMENTO",
array_agg(a."HORA ATENDIMENTO"[1] ORDER BY a."ID HISTORICO" DESC) AS "HORA ATENDIMENTO",
array_agg(a."ATENDENTE"[1] ORDER BY a."ID HISTORICO" DESC) AS "ATENDENTE",
a."ASSUNTO",
array_agg(a."DESCRICAO ATENDIMENTO"[1] ORDER BY a."ID HISTORICO" DESC) AS "DESCRICAO ATENDIMENTO",
array_agg(a."Valor" ORDER BY a."ID HISTORICO" DESC) AS "VALOR ADITIVO"
FROM ( SELECT r.descricao AS "REGIONAL",
cid.nomedacidade AS "CIDADE",
ct.id AS "ID CONTRATO",
ct.contrato AS "CONTRATO",
array_agg(hg.id) AS "ID HISTORICO",
array_agg(hg.d_data) AS "DATA ATENDIMENTO",
array_agg(date_trunc('second'::text, hg.t_hora::interval)::text) AS "HORA ATENDIMENTO",
array_agg(hg.atendente) AS "ATENDENTE",
gh.descricao AS "ASSUNTO",
array_agg(ah.descricao) AS "DESCRICAO ATENDIMENTO",

sum(func_calculavaloraditivos_v2(ct.cidade, ct.codempresa, ct.contrato, pg.tipoponto, pg.tipoprogramacao, cp.valorpacote, date_trunc('month',hg.d_data - interval '1 month')::date, (date_trunc('month',(hg.d_data))-interval '1 day')::date, cp.protabelaprecos))
as "Valor"

FROM historicogeral hg
JOIN assuntohistorico ah ON hg.grupoassunto = ah.codigogrupo AND hg.assunto = ah.codigoassunto
JOIN grupohistorico gh ON hg.grupoassunto = gh.codigo
JOIN contratos ct ON ct.cidade = hg.codigocidade AND ct.contrato = hg.codcontrato
JOIN cidade cid ON cid.codigodacidade = ct.cidade
JOIN regional r ON r.codigo = cid.codigo_regional
JOIN cont_prog cp ON cp.cidade = ct.cidade AND cp.contrato = ct.contrato
LEFT JOIN programacao pg ON pg.codigodaprogramacao = cp.protabelaprecos AND pg.codcidade = cp.cidade

-- ########## ALTERAR A DATA AQUI ###########

WHERE hg.d_data >= '01-03-2022' and hg.d_data <= '31-03-2022'

AND hg.historicopai IS NULL AND (hg.grupoassunto = ANY (ARRAY[851, 881]))
GROUP BY r.descricao, cid.nomedacidade, ct.id, ct.contrato, hg.id, gh.descricao, hg.grupoassunto, hg.historicopai) a
GROUP BY a."REGIONAL", a."CIDADE", a."ID CONTRATO", a."CONTRATO", a."ASSUNTO") b;

------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.estoque_movimentacao_periodo (
  pdatainicial date,
  pdatafinal date
)
RETURNS TABLE (
  datamovimentacao date,
  tipomovimentacao text,
  numeromiventacao bigint,
  armazemorigem text,
  armazemdestino text,
  codigoproduto bigint,
  descricaoproduto text,
  nomeservico text,
  ordemservico bigint,
  contrato bigint,
  quantidade integer
) AS
$body$
begin
  return query
  select mv.d_datacadastro::date,
    case
      when mv.codigopedido is not null then 'Pedido'::text
      when mv.codigorequisicao is not null then 'Requisição - Entrada'::text
      when position('REQ'::text in mv.idorigem::text) > 0 then 'Requisição - Saída'::text
      when mv.idmateriaisos is not null then 'Materiais Utilizados - Entrada'::text
      when position('MTU'::text in mv.idorigem::text) > 0 then 'Materiais Utilizados - Saída'::text
      when mv.idmateriaisretirados is not null then 'Materiais Retirados - Entrada'::text
      when position('MTR'::text in mv.idorigem::text) > 0 then 'Materiais Retirados - Saída'::text
      when mv.codtransferencia is not null then 'Transferência - Entrada'::text
      when position('TRF'::text in mv.idorigem::text) > 0 then 'Transferência - Saída'::text
      when mv.coddevolucao is not null then 'Devolução - Entrada'::text
      when position('DEV'::text in mv.idorigem::text) > 0 then 'Devolução - Saída'::text
    end,
    mv.codigo::bigint, a1.descricao::text, a.descricao::text, mv.codigoproduto::bigint, prd.descricao::text,
    case when l.id is not null then l.descricaodoserv_lanc::text else l1.descricaodoserv_lanc::text end,
    case when os.id is not null then os.numos::bigint else os1.numos::bigint end,
    case when os.id is not null then os.codigocontrato::bigint else os1.codigocontrato::bigint end,
    mv.quantidade::integer
  from public.movimentacaoproduto mv
  join public.produtos prd on prd.codigo = mv.codigoproduto
  join public.armazem a on a.codigo = mv.codarmazem
  left join public.movimentacaoproduto mv1 on case
    when mv.idorigem is not null then mv1.id = btrim(substr(mv.idorigem,4,20))::bigint
    else btrim(substr(mv1.idorigem,4,20))::bigint = mv.id end
  left join public.armazem a1 on a1.codigo = mv1.codarmazem
  left join public.materiaisos mtu on mtu.id = mv.idmateriaisos
  left join public.materiaisosretirada mtr on mtr.id = mv.idmateriaisretirados
  left join public.ordemservico os on os.cidade = mtu.codigocidade and os.codempresa = mtu.codempresa and os.numos = mtu.numos
  left join public.ordemservico os1 on os1.cidade = mtr.codigocidade and os1.codempresa = mtr.codempresa and os1.numos = mtr.numos
  left join public.lanceservicos l on l.codigodoserv_lanc = os.codservsolicitado
  left join public.lanceservicos l1 on l1.codigodoserv_lanc = os1.codservsolicitado
  where mv.d_datacadastro between pDataInicial and pDataFinal
  order by mv.id;
end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100 ROWS 1000;

ALTER FUNCTION public.estoque_movimentacao_periodo (pdatainicial date, pdatafinal date)
  OWNER TO hilton;

------------------------------------------------------------------------------------------------------------------------------------------

select ct.codigodocliente,
nome,
plano,
valor,
datavencimento,
datapgto,
contacredito

FROM contratos ct

------------------------------------------------------------------------------------------------------------------------------------------

select dr.codigodacidade,
       dr.cliente,
       cl.nome,
       array_agg(split_part(m.observacao, '[', 1)) as "observacaoo",
       dr.valordocumento,
       dr.d_datavencimento,
       dr.d_datafaturamento,
       dr.d_datapagamento,
       ct.codcontacredito
FROM contratos ct
     LEFT JOIN docreceber dr on dr.cliente = ct.codigodocliente and dr.codigodacidade = ct.cidade
     JOIN clientes cl ON cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente
     JOIN contascreditocidade cc ON cc.codigocidade = dr.codigodacidade and cc.codigoconta = ct.codcontacredito
     JOIN movimfinanceiro m on m.numfatura = dr.fatura
WHERE dr.d_datafaturamento IS NOT NULL and
      dr.d_datafaturamento BETWEEN '2022-05-01' and '2022-05-31' and
      ct.codcontacredito <> '41'
GROUP BY dr.codigodacidade,
         dr.cliente,
         cl.nome,
         dr.valordocumento,
         dr.d_datavencimento,
         dr.d_datafaturamento,
         dr.d_datapagamento,
         ct.codcontacredito
ORDER BY cl.nome

------------------------------------------------------------------------------------------------------------------------------------------

--ASSINANTES COM VALOR ZERADO
select distinct
	   c.nomedacidade as "Nome_Assinante",
       cl.codigocliente as "Cod_Assinante",
       cl.nome as "Nome_Assinante", 
       ct.contrato as "Contrato",
case
          when ct.situacao = 1 then 'Aguardando Conexão'
          when ct.situacao = 2 then 'Conectado'
          when ct.situacao = 3 then 'Pausado'
          when ct.situacao = 4 then 'Inadimplente'
          when ct.situacao = 5 then 'Cancelado'
          when ct.situacao = 6 then 'Endereço não Cabeado'
          when ct.situacao = 7 then 'Conectado/Inadimplente'
end as "Situacao_Contrato",
       pr.codigodaprogramacao as "Cod_Programacao",
       pr.nomedaprogramacao as "Nome_Programacao",
case  
      when cp.situacao = 1 then 'Ligado'
      when cp.situacao = 2 then 'Desligado'
end as "Situacao_Pacote",
      p.nomedoponto as "Nome_Ponto",
      cp.valorpacote as "Valor"
from contratos ct
     join cidade c on c.codigodacidade = ct.cidade
     join clientes cl on cl.cidade = ct.cidade and cl.codigocliente = ct.codigodocliente
     join cont_prog cp on cp.cidade = ct.cidade and cp.codempresa = ct.codempresa and cp.contrato = ct.contrato
     join programacao pr on pr.codcidade = cp.cidade and pr.codigodaprogramacao = cp.protabelaprecos
     join pontos p on p.cidade = cp.cidade and p.codempresa = cp.codempresa and p.contrato = cp.contrato and p.numerodoponto = cp.codigodoponto
where ct.situacao <> 5 and cp.valorpacote = 0 
order by c.nomedacidade,
         cl.nome

--------------------------------------------------------------------------------------------------------------------------------------------

--ALTERAR PARÂMETROS DE BOLETO
select * from parametrochavevalor

--------------------------------------------------------------------------------------------------------------------------------------------

--CLIENTES SEM GERAÇÃO DE BOLETO
select c.nomedacidade,
       ct.codigodocliente,
       cl.nome,
       ct.contrato,
       CASE
         when ct.tipodocontrato = 221 then 'UNIMED - JAU'
         when ct.tipodocontrato = 231 then 'CONTRATO MINEIROS'
         when ct.tipodocontrato = 211 then 'PERMUTA'
         when ct.tipodocontrato = 271 then 'PERMUTA'
       end as tipodocontrato,
       case
         when ct.situacao = 1 then 'Aguardando Conexão'
         when ct.situacao = 2 then 'Conectado'
         when ct.situacao = 3 then 'Pausado'
         when ct.situacao = 4 then 'Inadimplente'
         when ct.situacao = 5 then 'Cancelado'
         when ct.situacao = 6 then 'Endereço não Cabeado'
         when ct.situacao = 7 then 'Conectado/Inadimplente'
       end as "Situacao_Contrato"
from contratos ct
     join cidade c on c.codigodacidade = ct.cidade
     join clientes cl on cl.cidade = ct.cidade and cl.codigocliente = ct.codigodocliente
where ct.tipodocontrato IN (221, 231, 211, 271) and
      ct.situacao <> 5

--------------------------------------------------------------------------------------------------------------------------------------------

select t.*
from (
       select codigopedido,
              codigoproduto,
              descricaoproduto,
              datamovimentacao,
              tipomovimentacao,
              armazemorigem,
              armazemdestino,
              numeromiventacao
       from regrasoperacao.estoque_movimentacao_periodo('2022-05-01'::date, '2022-05-01'::date, 27::smallint)
     ) as t

--------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW regrasoperacao.vis_relatorio_faturamento_contacredito(
    nomecidade,
    codigocliente,
    nomecliente,
    observacaoboleto,
    numerodocumento,
    valordocumento,
    datavencimento,
    datafaturamento,
    datapagamento,
    localdecobranca,
    descricao,
    codigocontacredito,
    descricaocontacredito)
AS
  SELECT c.nomedacidade AS nomecidade,
         dr.cliente AS codigocliente,
         cl.nome AS nomecliente,
         array_agg(split_part(m.observacao::text, '['::text, 1)) AS observacaoboleto,
         dr.numerodocumento,
         dr.valordocumento,
         dr.d_datavencimento AS datavencimento,
         dr.d_datafaturamento AS datafaturamento,
         dr.d_datapagamento AS datapagamento,
         dr.localcobranca AS localdecobranca,
         l.descricao,
         dr.codcontacredito AS codigocontacredito,
         cc.conta_descricao AS descricaocontacredito
  FROM contratos ct
       LEFT JOIN docreceber dr ON dr.cliente = ct.codigodocliente AND dr.codigodacidade = ct.cidade
       JOIN cidade c ON c.codigodacidade = dr.codigodacidade
       JOIN clientes cl ON cl.cidade = dr.codigodacidade AND cl.codigocliente = dr.cliente
       JOIN localcobranca l ON l.codigo = dr.localcobranca
       JOIN contascreditocidade cc ON cc.codigocidade = dr.codigodacidade AND cc.codigoconta = dr.codcontacredito
       JOIN movimfinanceiro m ON m.numfatura = dr.fatura
  WHERE dr.d_datafaturamento IS NOT NULL
  GROUP BY c.nomedacidade,
           dr.cliente,
           cl.nome,
           dr.valordocumento,
           dr.d_datavencimento,
           dr.d_datafaturamento,
           dr.d_datapagamento,
           dr.codcontacredito,
           dr.localcobranca,
           l.descricao,
           dr.numerodocumento,
           cc.conta_descricao
  ORDER BY c.nomedacidade,
           cl.nome;

--------------------------------------------------------------------------------------------------------------------------------------------

select c.nomedacidade,
       dr.cliente,
       cl.nome,
       array_agg(split_part(m.observacao, '[', 1)) as "observacao_boleto",
       dr.numerodocumento,
       dr.valordocumento,
       dr.d_datavencimento,
       dr.d_datafaturamento,
       dr.d_datapagamento,
       dr.localcobranca,
       l.descricao,
       dr.codcontacredito,
       cc.conta_descricao
FROM contratos ct
     LEFT JOIN docreceber dr on dr.cliente = ct.codigodocliente and dr.codigodacidade = ct.cidade
     join cidade c on c.codigodacidade = dr.codigodacidade
     JOIN clientes cl ON cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente
     join localcobranca l on l.codigo = dr.localcobranca
     JOIN contascreditocidade cc ON cc.codigocidade = dr.codigodacidade and cc.codigoconta = dr.codcontacredito
     JOIN movimfinanceiro m on m.numfatura = dr.fatura
WHERE dr.d_datafaturamento IS NOT NULL and
      dr.d_datafaturamento BETWEEN '2022-05-01' and '2022-05-31'
GROUP BY c.nomedacidade,
         dr.cliente,
         cl.nome,
         dr.valordocumento,
         dr.d_datavencimento,
         dr.d_datafaturamento,
         dr.d_datapagamento,
         dr.codcontacredito,
         dr.localcobranca,
         l.descricao,
         dr.numerodocumento,
         cc.conta_descricao
ORDER BY c.nomedacidade,
         cl.nome

--------------------------------------------------------------------------------------------------------------------------------------------

with 
	x as (SELECT ct_1.id, ls_1.descricaodoserv_lanc, count(os_1.id) as qtde, os_1.d_dataexecucao
	FROM ordemservico os_1
	JOIN lanceservicos ls_1 ON ls_1.codigodoserv_lanc = os_1.codservsolicitado
	JOIN contratos ct_1 ON ct_1.cidade = os_1.cidade AND ct_1.codempresa = os_1.codempresa AND ct_1.contrato = os_1.codigocontrato
	where os_1.codservsolicitado = 1271 
	group by ct_1.id, ls_1.descricaodoserv_lanc, d_dataexecucao ), 
    y as (SELECT ct_2.id, ls_2.descricaodoserv_lanc, count(os_2.id) as qtde, os_2.d_dataexecucao 
	FROM ordemservico os_2
	JOIN lanceservicos ls_2 ON ls_2.codigodoserv_lanc = os_2.codservsolicitado
	JOIN contratos ct_2 ON ct_2.cidade = os_2.cidade AND ct_2.codempresa = os_2.codempresa AND ct_2.contrato = os_2.codigocontrato
	where os_2.codservsolicitado = 1361 
	group by ct_2.id, ls_2.descricaodoserv_lanc, os_2.d_dataexecucao ) 
    select distinct cli.codigocliente, cli.nome, func_retornatelefones(ct.cidade, ct.codigodocliente) as telefones, x.qtde as "Quantidade de VERIFICAÇÃO DE IRREGULARIDADES- INTERNET",
    x.d_dataexecucao, y.qtde as "Quantidade de VERIFICAÇÃO NO LOCAL",  y.d_dataexecucao
from ordemservico os
JOIN lanceservicos l ON l.codigodoserv_lanc = os.codservsolicitado
JOIN cidade cid ON cid.codigodacidade = os.cidade
JOIN clientes cli ON cli.cidade = os.cidade AND cli.codigocliente = os.codigoassinante
JOIN contratos ct ON ct.cidade = os.cidade AND ct.codempresa = os.codempresa AND ct.contrato = os.codigocontrato
join x on x.id=ct.id
join y on y.id=ct.id

--------------------------------------------------------------------------------------------------------------------------------------------

 SELECT * FROM OPENQUERY

select * from integracao.totvs_boleto
WHERE
tipotransacao='U'
--AND situacao = 0
--and idbase=2
and idboleto in(
10345193,
10225400,
10359559
        )
        
select * from auditoria.aud_historicogeral l

WHERE id = 70

select * from auditoria.aud_assuntohistorico a
 where a.codigoassunto = 31 and a.codigogrupo = 291      
     

     
 select * from auditoria.aud_historicogeral l
 where l.id = 47725449
      
      
 select * from historicogeral l
 where l.codigocidade = 329721 and l.assinante = 739631 
      
 select * from cidade
 
 
 select * from campanhadocreceber c
 where c.d_datacadastro = '2022-03-17'
 
select distinct a.id as "ID_assinatura", t.nome as "Cidade", a.nome as "Nome_cliente", a.cpfcnpj as "CPF_CNPJ", date(a.criadoem) as datacriacao,
date(a.dataprocessamento) as "Data_processamento", caa.descricao as "Classificação_andamento",
btrim(s.descricao) as situacao,
an.nome as analista,
pt.vis_nome_pacote as "Pacote", pt.vis_valor as "Valor_pacote", ta.nomedatabeladeprecos as "Tabela_preco"
from interfocusprospect.assinatura a
join interfocusprospect.usuariolocal u on u.id=a.captador
join interfocusprospect.statusassinatura s on s.id=a.statusassinatura
join public.tablocal t on t.id=a.municipioterceirosconexao
left join interfocusprospect.usuariolocal an on an.id=a.analistaid
join public.vendedores v on v.id=u.vendedorterceiros
join public.canaisdevenda ca on ca.cidade=v.cidadeondetrabalha and ca.codigo=v.canalvenda
join interfocusprospect.assinaturapacoteterceiros ap on ap.assinatura=a.id
JOIN interfocusprospect.vis_pacotetabela pt ON pt.vis_id =ap.pacoteterceiros
join tabeladeprecos ta on ta.id=pt.vis_id_tabela_preco
join interfocusprospect.assinaturaandamento aa on aa.assinatura = a.id
join interfocusprospect.classificacaoandamento caa on caa.id = aa.classificacaoandamento
where date(a.criadoem) between '2022-01-01' and '2022-03-18'
order by t.nome, a.id

select distinct a.id as "ID_assinatura", t.nome as "Cidade", a.nome as "Nome_cliente", a.cpfcnpj as "CPF_CNPJ", date(a.criadoem) as datacriacao,
date(a.dataprocessamento) as "Data_processamento", caa.descricao as "Classificação_andamento",
btrim(s.descricao) as situacao, u.nome as "vendedor",
an.nome as analista,
pt.vis_nome_pacote as "Pacote", pt.vis_valor as "Valor_pacote", ta.nomedatabeladeprecos as "Tabela_preco"
from interfocusprospect.assinatura a
join interfocusprospect.usuariolocal u on u.id=a.captador
join interfocusprospect.statusassinatura s on s.id=a.statusassinatura
join public.tablocal t on t.id=a.municipioterceirosconexao
left join interfocusprospect.usuariolocal an on an.id=a.analistaid
join public.vendedores v on v.id=u.vendedorterceiros
join public.canaisdevenda ca on ca.cidade=v.cidadeondetrabalha and ca.codigo=v.canalvenda
join interfocusprospect.assinaturapacoteterceiros ap on ap.assinatura=a.id
JOIN interfocusprospect.vis_pacotetabela pt ON pt.vis_id =ap.pacoteterceiros
join tabeladeprecos ta on ta.id=pt.vis_id_tabela_preco
join interfocusprospect.assinaturaandamento aa on aa.assinatura = a.id
join interfocusprospect.classificacaoandamento caa on caa.id = aa.classificacaoandamento
where date(a.criadoem) between '2022-03-01' and '2022-03-22'
order by t.nome, a.id

--------------------------------------------------------------------------------------------------------------------------------------------

Select c.nomedacidade as "Cidade",
       cl.codigocliente as "Cod_Assinante",
       cl.nome as "Nome_Assinante",
       dr.numerodocumento as "Numero_Documento",
       dr.d_datavencimento as "Data_Vencimento",
       dr.nossonumero as "Nosso_Numero",
       cc.codigoconta as "Conta_Crédito",
       b.nome as "Banco"
from docreceber dr
     join public.cidade c on c.codigodacidade = dr.codigodacidade
     join public.clientes cl on cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente
     join public.contascreditocidade cc on cc.codigocidade = dr.codigodacidade and cc.codigoconta = dr.codcontacredito
     join public.bancos b on b.numero = cc.banco
where dr.d_datavencimento = '2022-04-04' and  dr.nossonumero in (10752972, 10774412, 10823572, 10844942, 10856572,
  10857222, 10864442, 10869642, 10874612, 10877132, 10893042, 10899792,
  10903542, 10907702, 12061322, 10988052, 10991702, 11005922, 11020692,
  11066782, 11114342, 11086772, 11099432, 11101972, 11103872, 11108802,
  11108902, 11111362, 11176652, 11189152, 11272752, 11221402, 11506702,
  11225192, 11192132, 11224902, 11195232, 11261572, 11320152, 11219252,
  11324802, 11285172, 11329602, 11288312, 11289152, 11208722, 11334622,
  11254932, 11410902, 11307322, 11357582, 11465772, 11371772, 11375202,
  11375862, 11383472, 11390672, 11401242, 11401332, 11411322, 11411902,
  11413082, 11414832, 11417002, 11417142, 11417452, 11417952, 11419822,
  11420042, 11422152, 11422412, 11422672, 11426172, 11426332, 11426552,
  11430382, 11430542, 11432512, 11433332, 11433702, 11435322, 11435812,
  11442782, 11443952, 11446652, 11449952, 11455912, 11462722, 11476742,
  11466362, 11466812, 11473182, 11477522, 11482912, 11486692, 11487302,
  11487592, 11488862, 11489662, 11494032, 11495052, 11496432, 11504272,
  11506102, 11506262, 11509182, 11509212, 11586302, 11614982, 11622122,
  11618972, 11620982, 11627142, 11628602, 11636892, 11706472, 11714032,
  11747862, 11768252, 11768282, 11808262, 11812592, 11828062, 11830132,
  11944472, 11926432, 11934722, 11951942, 11952192, 11953052, 11954462,
  11995142, 11996512)
  order by c.nomedacidade

--------------------------------------------------------------------------------------------------------------------------------------------

select DISTINCT
       cl.codigocliente,
       ct.contrato,
       cl.nome, 
       array_to_string(ARRAY
       ( SELECT * FROM public.func_separaemail(public.valida_email(cl.email)) ), ','::text) as email,
       case
       when length(cl.cpf_cnpj) > 14 then 'JURIDICO'
       ELSE 'FISICO'
       end as tipopessoa
      
from public.clientes cl
     join public.telefones t on t.cidade = cl.cidade and t.codigocliente = cl.codigocliente
     left join public.contratos ct on ct.cidade = cl.cidade and ct.codigodocliente = cl.codigocliente
where public.valida_email(cl.email) is not null and
      ct.situacao in (2)

--------------------------------------------------------------------------------------------------------------------------------------------

--SVA MEGABIT--
SELECT ns.d_dataemissao as "data emissao",
ns.tiponf as "tipo nf",
ns.nomedacidade as "cidade",
ns.codigocliente as "cod. cliente",
ns.nome as "nome cliente",
ns.cpf_cnpj as "CPF_CNPJ",
ns.numnf as "numero nf",
ns.serienf as "serie nf",
ns.periodo as "periodo",
ns.totalnota as "total nota",
ns.baseicms as "base icms",
ns.valoricms as "valor icms",
ns.valorpis as "valor pis",
ns.valorcofins as "valor cofins",
ns.valorfust as "valor fust",
ns.valorfuntel as "valor funtel"
from regrasoperacao.vis_notas_sva ns
where ns.d_dataemissao between '2022-05-01' and '2022-05-31'

--------------------------------------------------------------------------------------------------------------------------------------------

select * from temporarias.func_executa_baixa_automatica_marretada()

--------------------------------------------------------------------------------------------------------------------------------------------

select c.nomedacidade,
       ct.codigodocliente,
       cl.nome,
       ct.contrato,
       CASE
         when ct.tipodocontrato = 221 then 'UNIMED - JAU'
         when ct.tipodocontrato = 231 then 'CONTRATO MINEIROS'
         when ct.tipodocontrato = 211 then 'PERMUTA'
         when ct.tipodocontrato = 271 then 'PERMUTA'
       end as tipodocontrato,
       case
         when ct.situacao = 1 then 'Aguardando Conexão'
         when ct.situacao = 2 then 'Conectado'
         when ct.situacao = 3 then 'Pausado'
         when ct.situacao = 4 then 'Inadimplente'
         when ct.situacao = 5 then 'Cancelado'
         when ct.situacao = 6 then 'Endereço não Cabeado'
         when ct.situacao = 7 then 'Conectado/Inadimplente'
       end as "Situacao_Contrato"
from contratos ct
     join cidade c on c.codigodacidade = ct.cidade
     join clientes cl on cl.cidade = ct.cidade and cl.codigocliente = ct.codigodocliente
     join docreceber dr on dr.cliente = ct.codigodocliente and dr.codigodacidade = ct.cidade
where ct.tipodocontrato IN (221, 231, 211, 271) and
      ct.situacao <> 5 and
      dr.d_datafaturamento = '2022-05-31'

UNION

select distinct c.nomedacidade,
       ct.codigodocliente,
       cl.nome,
       ct.contrato,
       case
         when ct.tipodocontrato = 11  then 'PADRAO*'
         when ct.tipodocontrato = 21  then 'AMELIABOCAINA'
         when ct.tipodocontrato = 81  then 'DESCONTODER$15,00'
         when ct.tipodocontrato = 101 then 'FUNCIONARIO-30%'
         when ct.tipodocontrato = 111 then 'FUNCIONARIO-50%'
         when ct.tipodocontrato = 141 then 'PADRÃO'
         when ct.tipodocontrato = 151 then 'PERMUTA'
         when ct.tipodocontrato = 171 then 'RESTAURANTECHULETA'
         when ct.tipodocontrato = 181 then 'ROQUEPAULUCCI'
         when ct.tipodocontrato = 191 then 'TVSTUDIODEJAUS/A'
         when ct.tipodocontrato = 211 then 'PERMUTA'
         when ct.tipodocontrato = 221 then 'UNIMED - JAU'
         when ct.tipodocontrato = 231 then 'CONTRATO MINEIROS'
         when ct.tipodocontrato = 241 then 'PADRAO 2'
         when ct.tipodocontrato = 251 then 'CONTRATOS ANUAIS'
         when ct.tipodocontrato = 261 then 'DIRECTV GO'
         when ct.tipodocontrato = 271 then 'PERMUTA'
       end as tipodocontrato,
       case
         when ct.situacao = 1 then 'Aguardando Conexão'
         when ct.situacao = 2 then 'Conectado'
         when ct.situacao = 3 then 'Pausado'
         when ct.situacao = 4 then 'Inadimplente'
         when ct.situacao = 5 then 'Cancelado'
         when ct.situacao = 6 then 'Endereço não Cabeado'
         when ct.situacao = 7 then 'Conectado/Inadimplente'
       end as "Situacao_Contrato"
from contratos ct
     join cidade c on c.codigodacidade = ct.cidade
     join clientes cl on cl.cidade = ct.cidade and cl.codigocliente = ct.codigodocliente
     join docreceber dr on dr.cliente = ct.codigodocliente and dr.codigodacidade = ct.cidade
where ct.situacao <> 5 and
      dr.nossonumerobanco is null and
      dr.d_datafaturamento = '2022-05-31'

--------------------------------------------------------------------------------------------------------------------------------------------

select c.nomedacidade as "Cidade",
       cl.nome as "Nome_Assinante"
from docreceber dr
     join cidade c on c.codigodacidade = dr.codigodacidade
     join public.clientes cl on cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente
where dr.situacao = 0 and
      dr.formadepagamento = 1 and
      dr.d_datapagamento is null
GROUP BY c.nomedacidade, cl.nome
HAVING sum(dr.valordocumento) <= 10.00
order by cl.nome


--------------------------------------------------------------------------------------------------------------------------------------------

--Planilha desconto sem serviço/conexão
select * from temporarias.inclui_desconto_horas_tiraprog(
	839501,
    105,
    '2022-05-25',
    '1441,1451,1301',
    'INDISPONIBILIDADE SERVIÇOS DE INTERNET - 25/05/2022',
    1
)

--------------------------------------------------------------------------------------------------------------------------------------------

update docreceber
set situacao = 1, d_datacancelamento = '2022-06-07', motivocancelamento = 1381
where id in (30638, 323213, 296232, 41461, 97865, 73722, 127018, 152756, 219870, 218424, 105078, 170751, 107887, 254511, 37057, 22822, 252263, 76397,
  102505, 80346, 75152, 114316, 89441, 53103, 230827, 136041, 136116, 100117, 121550, 43406, 37372, 144461, 91423, 152131, 50965, 110395, 35043,
  102618, 87395, 328150, 127794, 136309, 172869, 142937, 7732, 129796, 322339, 118093, 171156, 103372, 156007, 92753, 84670, 203520, 321490, 101034,
  119420, 195103, 20145, 15945, 146617, 331705, 127175, 73514, 240253, 43680, 262416, 154441, 128874, 75318, 158424, 149831, 53012, 124986, 100935,
  76255, 124744, 271638, 14921, 42049, 126170, 202108, 168388, 82563, 109777, 40998, 97849, 95760, 43599, 40243, 98594, 305316, 166581, 125847,
  156274, 19905, 153113, 339273, 144048, 80123, 96274, 43082, 230003, 157785, 198680, 44450, 131387, 137551, 98406, 118040, 135753, 351740, 124480,
  243941, 51731, 38250, 134565, 111076, 355185, 33495, 127623, 102263, 154718, 102309, 22982, 125299, 344719, 103445, 150670, 157381, 136104, 169484,
  211891, 367547, 120157, 174462, 148290, 285082, 298798, 78517, 38109, 30572, 129705, 323164, 149797, 20454, 41383, 79365, 44417, 30740, 141912,
  167550, 136873, 26741, 10923, 27868, 218940, 86980, 119216, 109847, 9277, 115898, 21383, 194816, 45918, 143282, 33110, 137311, 333815, 6722, 308352,
  266882, 74336, 119526, 46571, 360541, 147258, 13423, 46593, 156265, 131268, 229601, 306711, 46801, 161061, 91759, 33823, 333094, 99944, 138330,
  156749, 51713, 438246, 102570, 128802, 89614, 97126, 53112, 31970, 115828, 98872, 79240, 97949, 136753, 129645, 85476, 38142, 93459, 118981, 101194,
  120322, 119160, 90107, 344620, 130459, 22991, 119263, 20743, 129269, 112412, 102194, 18499, 264542, 113482, 40347, 161927, 118105, 105733, 119906,
  27939, 127918, 146259, 40452, 151753, 103739, 25423, 33715, 81197, 104911, 102277, 88172, 34822, 33688, 87591, 86543, 110726, 126763, 142379,
  119759, 253108, 109343, 112771, 42766, 127517, 93868, 173423, 102231, 111854, 103032, 10576, 36836, 105000, 220182, 33911, 129249, 168000, 146633,
  132745, 41463, 85237, 78206, 137463, 19527, 212102, 125003, 112062, 45658, 31934, 149908, 95059, 99893, 30158, 275268, 183245, 33099, 102784,
  131386, 32770, 260042, 101029, 240683, 86975, 216267, 223929, 138561, 19608, 75320, 138905, 303378, 95467, 114094, 106720, 152203, 33550, 95459,
  164429, 46370, 76456, 130923, 32105, 201457, 58855, 163738, 144186, 329167, 118915, 105268, 184713, 111944, 144035, 52957, 300328, 43519, 120593,
  348399, 16532, 43550, 41487, 110316, 114961, 104083, 344171, 150297, 325572, 26278, 78573, 119665, 33516, 59173, 102633, 133333, 18622, 266897,
  137547, 137388, 141922, 7798, 108857, 170731, 95854, 98389, 131884, 44694, 97129, 104046, 85272, 98880, 191984, 88618, 108610, 90309, 101673,
  235147, 107797, 140493, 287011, 101378, 124144, 97921, 29895, 227188, 169328, 80647, 144207, 25644, 16170, 102602, 111554, 112434, 312160, 46214,
  177991, 87162, 24977, 190715, 79233, 112043, 175684, 104922, 82741, 231166, 333840, 119457, 194547, 206181, 113035, 192574, 199636, 81904, 264021,
  87923, 19366, 74253, 112825, 76101, 118587, 137250, 284319, 150873, 13784, 392627, 167113, 349018, 310559, 128659, 258356, 77449, 45051, 381647,
  80099, 104551, 149751, 54516, 9570, 270907, 369136, 233043, 31523, 114265, 237143, 111591, 115843, 337528, 54376, 36478, 130331, 112357, 206673,
  41385, 197814, 278298, 19455, 280278, 329980, 12727, 99626, 52487, 317765, 101282, 136949, 167201, 109776, 120820, 107162, 55008, 116735, 323655,
  325219, 87278, 125928, 141565, 148142, 242206, 348388, 81478, 236480, 42022, 38171, 136711, 133507, 100421, 42042, 139760, 76075, 286123, 16929,
  349102, 256917, 97442, 116759, 181125, 142311, 118515, 84403, 118174, 42720, 159549, 75353, 111023, 229092, 29540, 147032, 140812, 73559, 17947,
  152188, 78721, 19894, 329937, 83671, 230040, 141475, 20676, 114594, 104691, 179313, 17030, 150278, 28864, 166332, 86826, 137270, 99284, 15133,
  125622, 150514, 138082, 81092, 282151, 272344, 109349, 128744, 39663, 108805, 96992, 13350, 151670, 472270, 151087, 471928, 204607, 125767, 38812,
  249175, 44561, 150758, 32958, 158366, 135845, 83791, 20250, 100499, 46780, 43060, 90238, 100094, 127509, 116855, 45990, 94625, 11158, 43905, 137554,
  100627, 12431, 33456, 103296, 103223, 191752, 40308, 108313, 121407, 154300, 118883, 24688, 21254, 185935, 137275, 362140, 227111, 152439, 294103,
  118993, 288675, 116713, 172358, 21825, 126937, 320996, 149099, 77509, 38930, 141470, 83364, 87543, 45430, 138336, 75863, 317309, 81080, 42532,
  112919, 73425, 147048, 86768, 235853, 21359, 127303, 132217, 126916, 42316, 151418, 102610, 130263, 220016, 143326, 129597, 107780, 15337, 34831,
  135677, 90588, 45067, 32123, 9258, 43400, 29685, 19000, 113598, 116692, 130975, 102624, 143904, 219202, 87585, 163839, 101435, 307430, 264073,
  89786, 86532, 94672, 196348, 270903, 32648, 20924, 96303, 112897, 27926, 131370, 34249, 133719, 105591, 120234, 170538, 21530, 41987, 135985, 42196,
  18233, 33970, 203216, 172375, 238803, 196148, 267796, 86643, 180396, 198036, 126580, 127568, 8000, 100248, 194708, 100362, 34055, 249249, 44404,
  37023, 263745, 115947, 166308, 248378, 130057, 213274, 46171, 50582, 111245, 145032, 219711, 166489, 137182, 12814, 333769, 40945, 96670, 43645,
  84451, 154463, 128567, 33235, 162407, 387219, 132123, 108140, 96418, 137614, 104959, 358272, 87004, 120142, 135967, 120228, 111326, 302180, 167248,
  182356, 293833, 33603, 190897, 302373, 239258, 9730, 125137, 130768, 118059, 77807, 14728, 240787, 118671, 33932, 40371, 11628, 115719, 113282,
  74637, 147055, 27969, 46352, 90864, 19138, 36153, 87729, 157069, 19395, 154977, 36663, 105414, 115353, 120917, 146470, 150123, 309135, 113692,
  143196, 110103, 91223, 230522, 185324, 319374, 165405, 52726, 136242, 93860, 25862, 138334, 126800, 126017, 135541, 79091, 109022, 99451, 197176,
  105040, 115354, 136516, 194028, 34119, 231144, 93810, 12235, 79192, 203850, 106465, 203935, 11094, 152607, 139780, 275985, 44389, 102284, 117921,
  211759, 113847, 154491, 191102, 96772, 137280, 372326, 39434, 126143, 172031, 15771, 97029, 101088, 30244, 137151, 34943, 23243, 118072, 122398,
  194228, 330138, 145169, 119357, 133405, 160035, 120625, 131182, 27954, 121536, 182154, 103164, 183107, 160695, 15644, 212816, 135880, 125749, 43541,
  113822, 186328, 89498, 21192, 317645, 154898, 212417, 118524, 145413, 20798, 314229, 152977, 166663, 88339, 302571, 137612, 312113, 338111, 198277,
  206082, 179285, 51618, 19828, 21128, 153761, 160547, 81267, 96029, 112028, 102964, 57570, 150537, 97474, 87158, 87898, 32944, 142188, 130733,
  294905, 40894, 22865, 90239, 128686, 31264, 137115, 156969, 122138, 7257, 110510, 40788, 46435, 95415, 51406, 138869, 10851, 130876, 127955, 21721,
  34443, 279709, 136157, 115567, 117946, 43002, 89933, 140221, 255904, 39942, 86646, 142520, 42685, 13490, 41717, 137494, 129024, 163096, 309059,
  148607, 161527, 44046, 23709, 234586, 145115, 40745, 19614, 171615, 7419, 46398, 43396, 126224, 52214, 333208, 87820, 21325, 46813, 108297, 104765,
  148396, 102680, 290518, 139593, 94592, 115569, 119056, 21618, 90113, 106185, 149711, 74610, 149703, 273328, 100001, 75020, 87906, 87259, 79333,
  93867, 134163, 185279, 6717, 45583, 74946, 131481, 367363, 43534, 102724, 146931, 140542, 127079, 14803, 94609, 23351, 346699, 102270, 351647,
  368408, 167116, 135569, 49797, 129445, 151239, 16857, 143990, 132997, 102089, 109981, 43963, 114339, 78378, 54548, 123760, 297505, 73927, 150140,
  42960, 252755, 124945, 117884, 22824, 98289, 84283, 79056, 143924, 23449, 336958, 92845, 148939, 7601, 144000, 121510, 21968, 99611, 147262, 11185,
  211943, 285160, 23158, 155823, 155207, 165191, 57848, 17333, 127609, 58812, 219853, 144112, 11174, 89020, 74352, 42513, 37777, 45253, 135721,
  278975, 82539, 43046, 37198, 18894, 88637, 105606, 139015, 187304, 39516, 191477, 30215, 337705, 288474, 324550, 117563, 140211, 41785, 157850,
  155514, 30403, 20167, 314731, 130249);

--------------------------------------------------------------------------------------------------------------------------------------------

select dr.d_datacancelamento, * from auditoria.aud_docreceber dr
where dr.nossonumero = 813372

--------------------------------------------------------------------------------------------------------------------------------------------

--CANCELAMENTOS BOLETOS CONEXÃO
update docreceber set situacao = 1, d_datacancelamento = '2022-06-07', motivocancelamento = 1381 where id in (

select DISTINCT
       /*c.nomedacidade as "Cidade",
       cl.id,
       cl.nome as "Nome_Assinante",
       cl.codigocliente,*/
       dr.id
from docreceber dr
     join cidade c on c.codigodacidade = dr.codigodacidade
     join public.clientes cl on cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente AND CL.ID NOT IN (319722, 260042)
where dr.situacao = 0 and
      dr.formadepagamento = 1 and
      dr.d_datapagamento is null
      and cl.id in (
            select DISTINCT cl.id
            from docreceber dr
            join cidade c on c.codigodacidade = dr.codigodacidade
            join public.clientes cl on cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente
            where dr.situacao = 0 and dr.formadepagamento = 1 and dr.d_datapagamento is null
            GROUP BY c.nomedacidade, cl.nome, cl.codigocliente, cl.id HAVING sum(dr.valordocumento) <= 10.00
))

--------------------------------------------------------------------------------------------------------------------------------------------

--NOTAS FISCAIS MATCHA
with 
x as(
select cl.codigocliente, cl.nome,nf.numcontrato, nf.numnf, nf.serienf  from nfviaunica nf
join clientes cl on cl.cidade = nf.codcidade and cl.codigocliente =  nf.codassinante
where nf.d_dataemissao BETWEEN '20200801' and '20200831' and nf.codcidade = 895401 and nf.serienf ='U'
),
y as (
select cl.codigocliente, cl.nome,nf.numcontrato, nf.numnf, nf.serienf  from nfviaunica nf
join clientes cl on cl.cidade = nf.codcidade and cl.codigocliente =  nf.codassinante
where nf.d_dataemissao BETWEEN '20200801' and '20200831' and nf.codcidade = 895401 and nf.serienf ='1'
)  
select x.codigocliente, x.nome,x.numcontrato, x.numnf as NF_Telecom, 'NF_Telecom'as serienf,
y.numnf as NF_Debito, 'NF_Debito' as serienf 
 from x 
join y on y.codigocliente = x.codigocliente
limit 10

--------------------------------------------------------------------------------------------------------------------------------------------

select distinct
c.nomedacidade as "Cidade",
cl.codigocliente as "Codigo_Cliente",
cl.nome as "Nome",
cl.cpf_cnpj as "Cpf_Cnpj",
func_retornatelefones(cl.cidade, cl.codigocliente) as "Telefone",
ct.contrato as "Contrato",
ed.nomelogradouro as "Endereco_Conexao",
ct.numeroconexao as "Numero_Conexao",
ct.cepconexao as "Cep_Conexao",
ct.bairroconexao as "Bairro_Conexao",
case
when ct.situacao = 1 then 'Aguardando Conexão'
when ct.situacao = 2 then 'Conectado'
when ct.situacao = 3 then 'Pausado'
when ct.situacao = 4 then 'Inadimplente'
when ct.situacao = 5 then 'Cancelado'
when ct.situacao = 6 then 'Endereço não Cabeado'
when ct.situacao = 7 then 'Conectado/Inadimplente'
end as Situacao_Contrato,
dr.d_datafaturamento as "Data_Fatauramento",
dr.numerodocumento as "Numero_Documento",
array_to_string(
array(
select distinct mf.observacao
from public.movimfinanceiro mf
where
mf.numfatura = dr.fatura
order by mf.observacao
),'- '
) as "Observação",
dr.valordocumento as "Valor_Documento",
dr.d_datavencimento as "Data_Vencimento"
from docreceber dr
join public.fatura f on f.numerofatura = dr.fatura
join public.contratos ct on ct.cidade = f.codigodacidade and ct.contrato = f.numerodocontrato
join public.clientes cl on cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente
join public.cidade c on c.codigodacidade = cl.cidade
join public.movimfinanceiro m on m.numfatura = dr.fatura
join public.vis_contratos_cancelados_relatorios ctc on ctc.id = ct.id
join public.enderecos ed on ed.codigodacidade = ct.cidade and ed.codigodologradouro = ct.enderecoconexao
where dr.d_datavencimento BETWEEN '2021-01-01' and '2021-12-31' and
dr.situacao = 0 and
dr.d_datapagamento is null and
ct.situacao = 5
Order by c.nomedacidade

--------------------------------------------------------------------------------------------------------------------------------------------

--NOTAS FISCAIS MEGABIT
select public.funcao_geracao_notas_fiscais_v1 (
  '{
    "tipoData": "1",
    "dataInicial": "2022-06-01",
    "dataFinal": "2022-06-30",
    "dataEmissao": "2022-06-09",
    "apagaSomenteDebito": "true",
    "nossoNumero": "6234341",
    "valorMaximo": "457000",
    "tipoPessoa": "1",
    "codigosCidades": "",
    "codigoUnificadora": "11"
  }'::json
);

--------------------------------------------------------------------------------------------------------------------------------------------

-- REAJUSTE TROCA DE TABELA OSWALDO CRUZ
SELECT c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, cp.id AS idcont_prog,
CASE WHEN ct.situacao = 1 THEN 'Aguardando Conexão'
    WHEN ct.situacao = 2 THEN 'Conectado/Ativo'
    WHEN ct.situacao = 3 THEN 'Pausado'
    WHEN ct.situacao = 4 THEN 'Inadimplente'
    WHEN ct.situacao = 5 THEN 'Cancelado'
    WHEN ct.situacao = 6 THEN 'Sem Cabeamento'
    WHEN ct.situacao = 7 THEN 'Conectado/Inadimplente'
END AS situacaodocontrato, ct.d_datadainstalacao,
pr.codigodaprogramacao, pr.nomedaprogramacao, cp.valorpacote,
CASE WHEN cp.situacao = 1 then 'Ativo' ELSE 'Inativo' END AS situacaopacote,
cp.d_dataalttabela, cp.codigodatabeladeprecos,
case when cp.codigodatabeladeprecos = 41 then 31 end as tabelanova
FROM public.cont_prog cp
JOIN public.contratos ct ON ct.cidade = cp.cidade AND ct.codempresa = cp.codempresa AND ct.contrato = cp.contrato
JOIN public.clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
JOIN public.cidade c ON c.codigodacidade = ct.cidade
JOIN public.programacao pr ON pr.codcidade = cp.cidade AND pr.codigodaprogramacao = cp.protabelaprecos
WHERE ct.situacao IN (2,3,4,7) AND
--extract(month from ct.d_datadainstalacao) = 8 and extract(year from ct.d_datadainstalacao) <= (2019 - 1) and
ct.d_datadainstalacao <='2021-05-31' AND
ct.cidade in (860611)
--and cp.d_dataalttabela <= '2018-08-31'
and cp.codigodatabeladeprecos in (41)
ORDER BY c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, pr.nomedaprogramacao

--INSERIR HISTÓRICO--
INSERT INTO public.historicogeral(id, codigocidade, assinante,d_data, t_hora, descricao, grupoassunto, assunto,
    atendente, usuario, d_datacadastro, t_horacadastro,d_dataconclusao, t_horaconclusao, d_datafechamento,t_horafechamento,codcontrato)
SELECT nextval('public.historicogeral_id_seq'), cp.cidade, cli.codigocliente, current_date, current_time, 'REAJUSTE MÊS 05', 221, 11,
'REAJUSTE', 'REAJUSTE', current_date, current_time, current_date, CURRENT_TIME, current_date, current_time, cp.contrato
FROM public.cont_prog cp
JOIN public.contratos ct ON ct.cidade = cp.cidade AND ct.codempresa = cp.codempresa AND ct.contrato = cp.contrato
JOIN public.clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
JOIN public.cidade c ON c.codigodacidade = ct.cidade
JOIN public.programacao pr ON pr.codcidade = cp.cidade AND pr.codigodaprogramacao = cp.protabelaprecos
WHERE ct.situacao IN (2,3,4,7) AND
--extract(month from ct.d_datadainstalacao) = 8 and extract(year from ct.d_datadainstalacao) <= (2019 - 1) and
ct.d_datadainstalacao <= '2021-05-31' AND
ct.cidade in (860611)
--and cp.d_dataalttabela <= '2018-08-31'
and cp.codigodatabeladeprecos in (41)
ORDER BY c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, pr.nomedaprogramacao;

--UPADTE ALTERAÇÃO DE TABELA--
update cont_prog set codigodatabeladeprecos = x.tabelanova, d_dataalttabela = current_date, valorpacote = x.valornovo
from (
  select t.*, ptb.valordaprogramacao as valornovo from (
    SELECT c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, cp.id AS idcont_prog,
    CASE WHEN ct.situacao = 1 THEN 'Aguardando Conexão'
        WHEN ct.situacao = 2 THEN 'Conectado/Ativo'
        WHEN ct.situacao = 3 THEN 'Pausado'
        WHEN ct.situacao = 4 THEN 'Inadimplente'
        WHEN ct.situacao = 5 THEN 'Cancelado'
        WHEN ct.situacao = 6 THEN 'Sem Cabeamento'
        WHEN ct.situacao = 7 THEN 'Conectado/Inadimplente'
    END AS situacaodocontrato, ct.d_datadainstalacao,
    pr.codigodaprogramacao, pr.nomedaprogramacao, cp.valorpacote,
    CASE WHEN cp.situacao = 1 then 'Ativo' ELSE 'Inativo' END AS situacaopacote,
    cp.d_dataalttabela, cp.codigodatabeladeprecos,
    case when cp.codigodatabeladeprecos = 41 then 31 end as tabelanova, cp.cidade
    FROM public.cont_prog cp
    JOIN public.contratos ct ON ct.cidade = cp.cidade AND ct.codempresa = cp.codempresa AND ct.contrato = cp.contrato
    JOIN public.clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
    JOIN public.cidade c ON c.codigodacidade = ct.cidade
    JOIN public.programacao pr ON pr.codcidade = cp.cidade AND pr.codigodaprogramacao = cp.protabelaprecos
    WHERE ct.situacao IN (2,3,4,7) AND
    --extract(month from ct.d_datadainstalacao) = 8 and extract(year from ct.d_datadainstalacao) <= (2019 - 1) and
    ct.d_datadainstalacao <= '2021-05-31' AND
    ct.cidade in (860611)
    --and cp.d_dataalttabela <= '2018-08-31'
    and cp.codigodatabeladeprecos in (41)
    ORDER BY c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, pr.nomedaprogramacao
  ) as t
  join prodtabelapreco ptb on ptb.codcidade=t.cidade and ptb.codigodatabela=t.tabelanova and ptb.codigodaprogramacao=t.codigodaprogramacao
) as x
where x.idcont_prog=public.cont_prog.id;

--UPDATE VALOR--
update cont_prog set valorpacote = x.valornovo
from (
  select cp.id, pr.valordaprogramacao as valornovo
  from cont_prog cp
  join prodtabelapreco pr on pr.codcidade=cp.cidade and pr.codigodatabela=cp.codigodatabeladeprecos and pr.codigodaprogramacao=cp.protabelaprecos
  where cp.situacao = 1 and pr.valordaprogramacao <> cp.valorpacote
) as x
where x.id=public.cont_prog.id

--------------------------------------------------------------------------------------------------------------------------------------------

-- JUNQUEIRÓPOLIS
SELECT c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, cp.id AS idcont_prog,
CASE WHEN ct.situacao = 1 THEN 'Aguardando Conexão'
    WHEN ct.situacao = 2 THEN 'Conectado/Ativo'
    WHEN ct.situacao = 3 THEN 'Pausado'
    WHEN ct.situacao = 4 THEN 'Inadimplente'
    WHEN ct.situacao = 5 THEN 'Cancelado'
    WHEN ct.situacao = 6 THEN 'Sem Cabeamento'
    WHEN ct.situacao = 7 THEN 'Conectado/Inadimplente'
END AS situacaodocontrato, ct.d_datadainstalacao,
pr.codigodaprogramacao, pr.nomedaprogramacao, cp.valorpacote,
CASE WHEN cp.situacao = 1 then 'Ativo' ELSE 'Inativo' END AS situacaopacote,
cp.d_dataalttabela, cp.codigodatabeladeprecos,
case when cp.codigodatabeladeprecos = 31 then 21 end as tabelanova
FROM public.cont_prog cp
JOIN public.contratos ct ON ct.cidade = cp.cidade AND ct.codempresa = cp.codempresa AND ct.contrato = cp.contrato
JOIN public.clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
JOIN public.cidade c ON c.codigodacidade = ct.cidade
JOIN public.programacao pr ON pr.codcidade = cp.cidade AND pr.codigodaprogramacao = cp.protabelaprecos
WHERE ct.situacao IN (2,3,4,7) AND
--extract(month from ct.d_datadainstalacao) = 8 and extract(year from ct.d_datadainstalacao) <= (2019 - 1) and
  ct.d_datadainstalacao <= '2021-05-31' and 
  ct.cidade in (847351)
--and cp.d_dataalttabela <= '2018-08-31'
and cp.codigodatabeladeprecos in (31)
ORDER BY c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, pr.nomedaprogramacao

--------INSERIR HISTÓRICO-----------
INSERT INTO public.historicogeral(id, codigocidade, assinante,d_data, t_hora, descricao, grupoassunto, assunto,
    atendente, usuario, d_datacadastro, t_horacadastro,d_dataconclusao, t_horaconclusao, d_datafechamento,t_horafechamento,codcontrato)
SELECT nextval('public.historicogeral_id_seq'), cp.cidade, cli.codigocliente, current_date, current_time, 'REAJUSTE MÊS 05', 221, 11,
'REAJUSTE', 'REAJUSTE', current_date, current_time, current_date, CURRENT_TIME, current_date, current_time, cp.contrato
FROM public.cont_prog cp
JOIN public.contratos ct ON ct.cidade = cp.cidade AND ct.codempresa = cp.codempresa AND ct.contrato = cp.contrato
JOIN public.clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
JOIN public.cidade c ON c.codigodacidade = ct.cidade
JOIN public.programacao pr ON pr.codcidade = cp.cidade AND pr.codigodaprogramacao = cp.protabelaprecos
WHERE ct.situacao IN (2,3,4,7) AND
--extract(month from ct.d_datadainstalacao) = 8 and extract(year from ct.d_datadainstalacao) <= (2019 - 1) and
ct.d_datadainstalacao <= '2021-05-31' and 
ct.cidade in (847351)
--and cp.d_dataalttabela <= '2018-08-31'
and cp.codigodatabeladeprecos in (31)
ORDER BY c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, pr.nomedaprogramacao


-----------update troca de tabela-----------
update cont_prog set codigodatabeladeprecos = x.tabelanova, d_dataalttabela = current_date, valorpacote = x.valornovo
from (
  select t.*, ptb.valordaprogramacao as valornovo from (
    SELECT c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, cp.id AS idcont_prog,
    CASE WHEN ct.situacao = 1 THEN 'Aguardando Conexão'
        WHEN ct.situacao = 2 THEN 'Conectado/Ativo'
        WHEN ct.situacao = 3 THEN 'Pausado'
        WHEN ct.situacao = 4 THEN 'Inadimplente'
        WHEN ct.situacao = 5 THEN 'Cancelado'
        WHEN ct.situacao = 6 THEN 'Sem Cabeamento'
        WHEN ct.situacao = 7 THEN 'Conectado/Inadimplente'
    END AS situacaodocontrato, ct.d_datadainstalacao,
    pr.codigodaprogramacao, pr.nomedaprogramacao, cp.valorpacote,
    CASE WHEN cp.situacao = 1 then 'Ativo' ELSE 'Inativo' END AS situacaopacote,
    cp.d_dataalttabela, cp.codigodatabeladeprecos,
    case when cp.codigodatabeladeprecos = 31 then 21 end as tabelanova, cp.cidade
    FROM public.cont_prog cp
    JOIN public.contratos ct ON ct.cidade = cp.cidade AND ct.codempresa = cp.codempresa AND ct.contrato = cp.contrato
    JOIN public.clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
    JOIN public.cidade c ON c.codigodacidade = ct.cidade
    JOIN public.programacao pr ON pr.codcidade = cp.cidade AND pr.codigodaprogramacao = cp.protabelaprecos
    WHERE ct.situacao IN (2,3,4,7) AND
    --extract(month from ct.d_datadainstalacao) = 8 and extract(year from ct.d_datadainstalacao) <= (2019 - 1) and
    ct.d_datadainstalacao <= '2021-05-31' and 
    ct.cidade in (847351)
    --and cp.d_dataalttabela <= '2018-08-31'
    and cp.codigodatabeladeprecos in (31)
    ORDER BY c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, pr.nomedaprogramacao
  ) as t
  join prodtabelapreco ptb on ptb.codcidade=t.cidade and ptb.codigodatabela=t.tabelanova and ptb.codigodaprogramacao=t.codigodaprogramacao
) as x
where x.idcont_prog=public.cont_prog.id;

--Update Valor--
update cont_prog set valorpacote = x.valornovo
from (
  select cp.id, pr.valordaprogramacao as valornovo
  from cont_prog cp
  join prodtabelapreco pr on pr.codcidade=cp.cidade and pr.codigodatabela=cp.codigodatabeladeprecos and pr.codigodaprogramacao=cp.protabelaprecos
  where cp.situacao = 1 and pr.valordaprogramacao <> cp.valorpacote
) as x
where x.id=public.cont_prog.id

--------------------------------------------------------------------------------------------------------------------------------------------

-- DRACENA
SELECT c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, cp.id AS idcont_prog,
CASE WHEN ct.situacao = 1 THEN 'Aguardando Conexão'
    WHEN ct.situacao = 2 THEN 'Conectado/Ativo'
    WHEN ct.situacao = 3 THEN 'Pausado'
    WHEN ct.situacao = 4 THEN 'Inadimplente'
    WHEN ct.situacao = 5 THEN 'Cancelado'
    WHEN ct.situacao = 6 THEN 'Sem Cabeamento'
    WHEN ct.situacao = 7 THEN 'Conectado/Inadimplente'
END AS situacaodocontrato, ct.d_datadainstalacao,
pr.codigodaprogramacao, pr.nomedaprogramacao, cp.valorpacote,
CASE WHEN cp.situacao = 1 then 'Ativo' ELSE 'Inativo' END AS situacaopacote,
cp.d_dataalttabela, cp.codigodatabeladeprecos,
case when pr.codclassificacao is not null and cp.codigodatabeladeprecos = 11 then 51
    when pr.codclassificacao is null and cp.codigodatabeladeprecos = 11 then 31
    when pr.codclassificacao is not null and cp.codigodatabeladeprecos = 21 then 61
    when pr.codclassificacao is null and cp.codigodatabeladeprecos = 21 then 71
    when cp.codigodatabeladeprecos = 31 then 71
    when cp.codigodatabeladeprecos = 51 then 61
    else 99
end as tabelanova
FROM public.cont_prog cp
JOIN public.contratos ct ON ct.cidade = cp.cidade AND ct.codempresa = cp.codempresa AND ct.contrato = cp.contrato
JOIN public.clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
JOIN public.cidade c ON c.codigodacidade = ct.cidade
JOIN public.programacao pr ON pr.codcidade = cp.cidade AND pr.codigodaprogramacao = cp.protabelaprecos
WHERE ct.situacao IN (2,3,4,7) AND
--extract(month from ct.d_datadainstalacao) = 8 and extract(year from ct.d_datadainstalacao) <= (2019 - 1) and
ct.d_datadainstalacao <= '2021-05-31' and
ct.cidade in (827401)
--and cp.d_dataalttabela <= '2018-08-31'
and cp.codigodatabeladeprecos not in (21,61,71)
and cp.protabelaprecos not in (141)
ORDER BY c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, pr.nomedaprogramacao


--INSERIR HISTÓRICO--
INSERT INTO public.historicogeral(id, codigocidade, assinante,d_data, t_hora, descricao, grupoassunto, assunto,
    atendente, usuario, d_datacadastro, t_horacadastro,d_dataconclusao, t_horaconclusao, d_datafechamento,t_horafechamento,codcontrato)
SELECT nextval('public.historicogeral_id_seq'), cp.cidade, cli.codigocliente, current_date, current_time, 'REAJUSTE MÊS 05', 221, 11,
'REAJUSTE', 'REAJUSTE', current_date, current_time, current_date, CURRENT_TIME, current_date, current_time, cp.contrato
FROM public.cont_prog cp
JOIN public.contratos ct ON ct.cidade = cp.cidade AND ct.codempresa = cp.codempresa AND ct.contrato = cp.contrato
JOIN public.clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
JOIN public.cidade c ON c.codigodacidade = ct.cidade
JOIN public.programacao pr ON pr.codcidade = cp.cidade AND pr.codigodaprogramacao = cp.protabelaprecos
WHERE ct.situacao IN (2,3,4,7) AND
--extract(month from ct.d_datadainstalacao) = 8 and extract(year from ct.d_datadainstalacao) <= (2019 - 1) and
ct.d_datadainstalacao <= '2021-05-31' and
ct.cidade in (827401)
--and cp.d_dataalttabela <= '2018-08-31'
and cp.codigodatabeladeprecos not in (21,61,71)
and cp.protabelaprecos not in (141)
ORDER BY c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, pr.nomedaprogramacao;

--UPDATE TROCA DE TABELA--
update cont_prog set codigodatabeladeprecos = x.tabelanova, d_dataalttabela = current_date
from (
  select t.*, ptb.valordaprogramacao from (
    SELECT c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, cp.id AS idcont_prog,
    CASE WHEN ct.situacao = 1 THEN 'Aguardando Conexão'
        WHEN ct.situacao = 2 THEN 'Conectado/Ativo'
        WHEN ct.situacao = 3 THEN 'Pausado'
        WHEN ct.situacao = 4 THEN 'Inadimplente'
        WHEN ct.situacao = 5 THEN 'Cancelado'
        WHEN ct.situacao = 6 THEN 'Sem Cabeamento'
        WHEN ct.situacao = 7 THEN 'Conectado/Inadimplente'
    END AS situacaodocontrato, ct.d_datadainstalacao,
    pr.codigodaprogramacao, pr.nomedaprogramacao, cp.valorpacote,
    CASE WHEN cp.situacao = 1 then 'Ativo' ELSE 'Inativo' END AS situacaopacote,
    cp.d_dataalttabela, cp.codigodatabeladeprecos,
    case when pr.codclassificacao is not null and cp.codigodatabeladeprecos = 11 then 51
        when pr.codclassificacao is null and cp.codigodatabeladeprecos = 11 then 31
        when pr.codclassificacao is not null and cp.codigodatabeladeprecos = 21 then 61
        when pr.codclassificacao is null and cp.codigodatabeladeprecos = 21 then 71
        when cp.codigodatabeladeprecos = 31 then 71
        when cp.codigodatabeladeprecos = 51 then 61
        else 99
    end as tabelanova, cp.cidade
    FROM public.cont_prog cp
    JOIN public.contratos ct ON ct.cidade = cp.cidade AND ct.codempresa = cp.codempresa AND ct.contrato = cp.contrato
    JOIN public.clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
    JOIN public.cidade c ON c.codigodacidade = ct.cidade
    JOIN public.programacao pr ON pr.codcidade = cp.cidade AND pr.codigodaprogramacao = cp.protabelaprecos
    WHERE ct.situacao IN (2,3,4,7) AND
    --extract(month from ct.d_datadainstalacao) = 8 and extract(year from ct.d_datadainstalacao) <= (2019 - 1) and
    ct.d_datadainstalacao  <= '2021-05-31' and
    ct.cidade in (827401)
    --and cp.d_dataalttabela <= '2018-08-31'
    and cp.codigodatabeladeprecos not in (21,61,71)
    and cp.protabelaprecos not in (141)
    ORDER BY c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, pr.nomedaprogramacao
  ) as t
  join prodtabelapreco ptb on ptb.codcidade=t.cidade and ptb.codigodatabela=t.tabelanova and ptb.codigodaprogramacao=t.codigodaprogramacao
) as x
where x.idcont_prog=public.cont_prog.id;


--UPDATE VALOR--
update cont_prog set valorpacote = x.valornovo
from (
  select cp.id, pr.valordaprogramacao as valornovo
  from cont_prog cp
  join prodtabelapreco pr on pr.codcidade=cp.cidade and pr.codigodatabela=cp.codigodatabeladeprecos and pr.codigodaprogramacao=cp.protabelaprecos
  where cp.situacao = 1 and pr.valordaprogramacao <> cp.valorpacote
) as x
where x.id=public.cont_prog.id

--------------------------------------------------------------------------------------------------------------------------------------------

-- INTERNET MAIS
-- CAMPO GRANDE--
SELECT c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, cp.id AS idcont_prog,
CASE WHEN ct.situacao = 1 THEN 'Aguardando Conexão'
    WHEN ct.situacao = 2 THEN 'Conectado/Ativo'
    WHEN ct.situacao = 3 THEN 'Pausado'
    WHEN ct.situacao = 4 THEN 'Inadimplente'
    WHEN ct.situacao = 5 THEN 'Cancelado'
    WHEN ct.situacao = 6 THEN 'Sem Cabeamento'
    WHEN ct.situacao = 7 THEN 'Conectado/Inadimplente'
END AS situacaodocontrato, a.codaditivo as "Cod_Aditivo", ad.descricao as "Descricao_Aditivo", a.d_datafim as "Fim_Aditivo", ct.d_datadainstalacao,
pr.codigodaprogramacao, pr.nomedaprogramacao, cp.valorpacote,
CASE WHEN cp.situacao = 1 then 'Ativo' ELSE 'Inativo' END AS situacaopacote,
cp.d_dataalttabela, cp.codigodatabeladeprecos,
case when cp.codigodatabeladeprecos = 11 then 31 end as tabelanova
FROM public.cont_prog cp
JOIN public.contratos ct ON ct.cidade = cp.cidade AND ct.codempresa = cp.codempresa AND ct.contrato = cp.contrato
JOIN PUBLIC.aditivoscontratos a on a.codcidade = ct.cidade and a.numcontrato = ct.contrato
join public.aditivos ad on ad.codaditivo = a.codaditivo
JOIN public.clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
JOIN public.cidade c ON c.codigodacidade = ct.cidade
JOIN public.programacao pr ON pr.codcidade = cp.cidade AND pr.codigodaprogramacao = cp.protabelaprecos
WHERE ct.situacao IN (2,3,4,7) AND
--extract(month from ct.d_datadainstalacao) = 8 and extract(year from ct.d_datadainstalacao) <= (2019 - 1) and
ct.d_datadainstalacao <= '2021-05-31' AND
ct.cidade = 402071
--and cp.d_dataalttabela <= '2018-08-31'
and cp.codigodatabeladeprecos in (11)
ORDER BY c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, pr.nomedaprogramacao



-- CAMPO GRANDE AERORANCHO
SELECT c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, cp.id AS idcont_prog,
CASE WHEN ct.situacao = 1 THEN 'Aguardando Conexão'
    WHEN ct.situacao = 2 THEN 'Conectado/Ativo'
    WHEN ct.situacao = 3 THEN 'Pausado'
    WHEN ct.situacao = 4 THEN 'Inadimplente'
    WHEN ct.situacao = 5 THEN 'Cancelado'
    WHEN ct.situacao = 6 THEN 'Sem Cabeamento'
    WHEN ct.situacao = 7 THEN 'Conectado/Inadimplente'
END AS situacaodocontrato, a.codaditivo as "Cod_Aditivo", ad.descricao as "Descricao_Aditivo", a.d_datafim as "Fim_Aditivo", ct.d_datadainstalacao,
pr.codigodaprogramacao, pr.nomedaprogramacao, cp.valorpacote,
CASE WHEN cp.situacao = 1 then 'Ativo' ELSE 'Inativo' END AS situacaopacote,
cp.d_dataalttabela, cp.codigodatabeladeprecos,
case when cp.codigodatabeladeprecos = 31 then 21 end as tabelanova
FROM public.cont_prog cp
JOIN public.contratos ct ON ct.cidade = cp.cidade AND ct.codempresa = cp.codempresa AND ct.contrato = cp.contrato
JOIN PUBLIC.aditivoscontratos a on a.codcidade = ct.cidade and a.numcontrato = ct.contrato
join public.aditivos ad on ad.codaditivo = a.codaditivo
JOIN public.clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
JOIN public.cidade c ON c.codigodacidade = ct.cidade
JOIN public.programacao pr ON pr.codcidade = cp.cidade AND pr.codigodaprogramacao = cp.protabelaprecos
WHERE ct.situacao IN (2,3,4,7) AND
--extract(month from ct.d_datadainstalacao) = 8 and extract(year from ct.d_datadainstalacao) <= (2019 - 1) and
ct.d_datadainstalacao <= '2021-05-31' AND
ct.cidade = 1085391
--and cp.d_dataalttabela <= '2018-08-31'
and cp.codigodatabeladeprecos in (31)
ORDER BY c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, pr.nomedaprogramacao

--INSERIR HISTÓRICO--
INSERT INTO public.historicogeral(id, codigocidade, assinante,d_data, t_hora, descricao, grupoassunto, assunto,
    atendente, usuario, d_datacadastro, t_horacadastro,d_dataconclusao, t_horaconclusao, d_datafechamento,t_horafechamento,codcontrato)
SELECT distinct nextval('public.historicogeral_id_seq'), cp.cidade, cli.codigocliente, current_date, current_time, 'REAJUSTE MÊS 05', 221, 11,
'REAJUSTE', 'REAJUSTE', current_date, current_time, current_date, CURRENT_TIME, current_date, current_time, cp.contrato
FROM public.cont_prog cp
JOIN public.contratos ct ON ct.cidade = cp.cidade AND ct.codempresa = cp.codempresa AND ct.contrato = cp.contrato
JOIN public.clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
JOIN public.cidade c ON c.codigodacidade = ct.cidade
JOIN public.programacao pr ON pr.codcidade = cp.cidade AND pr.codigodaprogramacao = cp.protabelaprecos
WHERE ct.situacao IN (2,3,4,7) AND
--extract(month from ct.d_datadainstalacao) = 8 and extract(year from ct.d_datadainstalacao) <= (2019 - 1) and
ct.d_datadainstalacao <= '2021-05-31' AND
ct.cidade in (402071, 1085391)
--and cp.d_dataalttabela <= '2018-08-31'
and cp.codigodatabeladeprecos in (11)

--UPDATE TROCA DE TABELA DE PREÇOS--
update cont_prog set codigodatabeladeprecos = x.tabelanova, d_dataalttabela = current_date, valorpacote = x.valornovo
from (
  select t.*, ptb.valordaprogramacao as valornovo from (
    SELECT c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, cp.id AS idcont_prog,
    CASE WHEN ct.situacao = 1 THEN 'Aguardando Conexão'
        WHEN ct.situacao = 2 THEN 'Conectado/Ativo'
        WHEN ct.situacao = 3 THEN 'Pausado'
        WHEN ct.situacao = 4 THEN 'Inadimplente'
        WHEN ct.situacao = 5 THEN 'Cancelado'
        WHEN ct.situacao = 6 THEN 'Sem Cabeamento'
        WHEN ct.situacao = 7 THEN 'Conectado/Inadimplente'
    END AS situacaodocontrato, ct.d_datadainstalacao,
    pr.codigodaprogramacao, pr.nomedaprogramacao, cp.valorpacote,
    CASE WHEN cp.situacao = 1 then 'Ativo' ELSE 'Inativo' END AS situacaopacote,
    cp.d_dataalttabela, cp.codigodatabeladeprecos,
    case when cp.codigodatabeladeprecos = 11 then 31 end as tabelanova, cp.cidade
    FROM public.cont_prog cp
    JOIN public.contratos ct ON ct.cidade = cp.cidade AND ct.codempresa = cp.codempresa AND ct.contrato = cp.contrato
    JOIN public.clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
    JOIN public.cidade c ON c.codigodacidade = ct.cidade
    JOIN public.programacao pr ON pr.codcidade = cp.cidade AND pr.codigodaprogramacao = cp.protabelaprecos
    WHERE ct.situacao IN (2,3,4,7) AND
    --extract(month from ct.d_datadainstalacao) = 8 and extract(year from ct.d_datadainstalacao) <= (2019 - 1) and
    ct.d_datadainstalacao <= '2021-05-31' AND
    ct.cidade in (402071, 1085391)
    --and cp.d_dataalttabela <= '2018-08-31'
    and cp.codigodatabeladeprecos in (11)
    ORDER BY c.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, pr.nomedaprogramacao
  ) as t
  join prodtabelapreco ptb on ptb.codcidade=t.cidade and ptb.codigodatabela=t.tabelanova and ptb.codigodaprogramacao=t.codigodaprogramacao
) as x
where x.idcont_prog=public.cont_prog.id;

--UPDATE AJUSTA PREÇOS--
update cont_prog set valorpacote = x.valornovo
from (
  select cp.id, pr.valordaprogramacao as valornovo
  from cont_prog cp
  join prodtabelapreco pr on pr.codcidade=cp.cidade and pr.codigodatabela=cp.codigodatabeladeprecos and pr.codigodaprogramacao=cp.protabelaprecos
  where cp.situacao = 1 and pr.valordaprogramacao <> cp.valorpacote
) as x
where x.id=public.cont_prog.id

--------------------------------------------------------------------------------------------------------------------------------------------

-- RELATÓRIO TEC - ESTOQUE - PROD/ARMAZÉM
with
per as (
  select Current_date::date as datainicial, current_date::date as datafinal
),
arm as (
  select a.codigo, a.descricao from public.armazem a where a.codigo = pCod
)
select res.codigoarmazem, res.descricaoarmazem,
  res.codigo, res.descricao, res.saldoinicial, res.entradas, res.saidas,
  res.saldoinicial + res.entradas + res.saidas as saldofinal
from (
  select est.codigoarmazem, est.descricaoarmazem,
    est.codigo, est.descricao, coalesce(est.sini,0) as saldoinicial, coalesce(est.entr,0) as entradas,
    coalesce(est.sai,0) as saidas
  from (
    select arm.codigo as codigoarmazem, arm.descricao as descricaoarmazem, p.codigo, p.descricao,
    (
      select sum(si.quantidade) as qtde from movimentacaoproduto si
      where si.codarmazem = arm.codigo and si.d_datacadastro < per.datainicial
        and si.codigoproduto = p.codigo
    ) as sini,
    (
      select sum(se.quantidade) as qtde from movimentacaoproduto se
      where se.codarmazem = arm.codigo and se.d_datacadastro between per.datainicial and per.datafinal
        and se.codigoproduto = p.codigo and se.quantidade > 0
    ) as entr,
    (
      select sum(ss.quantidade) as qtde from movimentacaoproduto ss
      where ss.codarmazem = arm.codigo and ss.d_datacadastro between per.datainicial and per.datafinal
        and ss.codigoproduto = p.codigo and ss.quantidade < 0
    ) as sai
    from public.produtos p
    join arm on true
    join per on true
    --where p.codigo = 3941
  ) as est
) res


where res.entradas <> 0 or res.saidas <> 0 or res.saldoinicial <> 0

--------------------------------------------------------------------------------------------------------------------------------------------

-- join movimfinanceiro
UPDATE movimfinanceiro m SET numnf = null, periodo=null, 
serienf=null, classificacao=null WHERE id IN (
 SELECT m1.id
 FROM movimfinanceiro m 
 JOIN movimfinanceiro m1 ON m1.cidade = m.cidade AND
m1.assinante = m1.assinante AND m.periodo::text = 
m1.periodo::text AND m.serienf::text = m1.serienf::text 
 AND m.numnf = m1.numnf AND m.codempresapacote = 
m1.codempresapacote AND m1.valoros * -1 = m.valoros
 WHERE m.valoros < 0 AND m.numnf IS NOT NULL)


-- distinct
INSERT INTO public.servequipe(
codcidade,
codequipe,
codservico,
usuario,
d_datacadastro,
t_horacadastro)
SELECT DISTINCT
e.codigocidade, 
e.codigodaequipe, 
3661, 
'SISTEMA', 
current_date, 
current_time
FROM equipe e 
LEFT JOIN servequipe s ON s.codcidade=e.codigocidade AND
s.codequipe=e.codigodaequipe
WHERE s.id IS NULL

DELETE FROM clientes WHERE id IN (
 SELECT cl.id FROM clientes cl
 ORDER BY cl.id DESC
 LIMIT 1000
)

-- join nfviaunica
SELECT * FROM (
 SELECT n.id, n.totalnota, SUM(m.valoros) as totalitens
 FROM nfviaunica n 
 JOIN movimfinanceiro m ON m.cidade = n.codcidade AND
m.assinante = n.codassinante AND m.periodo::text = 
n.periodo::TEXT AND m.serienf::TEXT = n.serienf::TEXT AND
m.numnf = n.numnf AND m.codempresapacote = n.codempresa
 WHERE n.d_dataemissao BETWEEN '2021-04-01' AND '2021-04-30'
 GROUP BY n.id, n.totalnota
) AS t
WHERE t.totalnota <> t.totalitens

-- having count
select x.codigocliente,cl.nome,x.nomedacidade,(x.total + 10) as total2 FROM(
  SELECT cl.codigocliente,ci.nomedacidade,COUNT(ct.contrato) as total FROM
  clientes cl
  JOIN contratos ct ON ct.codigodocliente = cl.codigocliente AND
  ct.cidade = cl.cidade
  JOIN cidade ci ON ci.codigodacidade = cl.cidade
  GROUP BY cl.codigocliente,ci.nomedacidade
  HAVING COUNT(ct.contrato) > 1
) as x
JOIN clientes cl on cl.codigocliente = x.codigocliente

SELECT dr.cliente,cl.nome,dr.id,count(dr.id) as total_boletos FROM docreceber dr
JOIN clientes cl ON cl.codigocliente = dr.cliente AND cl.cidade = 
dr.codigodacidade 
WHERE dr.d_datapagamento IS NOT NULL
GROUP BY dr.cliente,cl.nome,dr.id

-- coalesce
SELECT 
cli.nome,
cli.codigocliente,
coalesce(t.telefone,'NÃO TEM'),
cli.cidade 
FROM clientes cli
LEFT JOIN telefones t ON t.cidade = cli.cidade AND t.codigocliente 
= cli.codigocliente

-- count
SELECT count(*),FROM contratos ct
where ct.situacao = 2

SELECT count(ct.idcartaoipay) FROM contratos ct

-- sum
SELECT sum(dr.valorpago) FROM docreceber dr
WHERE dr.d_datavencimento BETWEEN '2022-06-01' AND '2022-06-30' AND dr.d_datapagamento IS NOT NULL

-- average
SELECT avg(dr.valorpago) FROM docreceber dr 
WHERE dr.d_datavencimento BETWEEN '20220601' AND '20220630' 
AND dr.d_datapagamento IS NOT NULL

-- cpf e cnpj
SELECT 
 cl.nome,
 cl.d_datanascimento,
 CASE
 WHEN length(translate(cl.cpf_cnpj::text, ' .,:-
//\_+='::text, ''::text)) = 14 THEN 'PJ'::text
 ELSE 'PF'::text
 END AS tipo_pessoa
FROM clientes cl

--------------------------------------------------------------------------------------------------------------------------------------------

--endereços
select * from enderecos ed
where ed.codigodacidade = 891681

--------------------------------------------------------------------------------------------------------------------------------------------

--EXERCÍCIO 1 TREINAMENTO POSTGRES

/*1) Trazer clientes e total de boletos
	codcliente,
    nome,
    cpf sem caracteres especiais,
	total de boletos*/
	
SELECT cl.codigocliente, cl.nome, translate(cl.cpf_cnpj::text, '.-/'::text, ''::text) as cpf, count(dr.id) as "qtde_boletos"
FROM docreceber dr
JOIN clientes cl ON cl.cidade = dr.codigodacidade AND cl.codigocliente = dr.cliente
group by cl.codigocliente, cl.nome, cpf

--------------------------------------------------------------------------------------------------------------------------------------------

-- EXERCÍCIO 1
/*1) Trazer clientes e total de boletos
	codcliente,
        nome,
        cpf sem caracteres especiais,
	total de boletos*/
    
SELECT cl.codigocliente, cl.nome, translate(cl.cpf_cnpj::text, '.-/'::text, ''::text) as cpf, count(dr.id) as "qtde_boletos"
FROM docreceber dr
JOIN clientes cl ON cl.cidade = dr.codigodacidade AND cl.codigocliente = dr.cliente
group by cl.codigocliente, cl.nome, cpf

--EXERCÍCIO 2
/*2) Trazer cliente, contrato e programacao em uma linha só
	codcliente,
        nome,
        cpf sem caracteres especiais,
        codcontrato,
        string com nome das programações*/

SELECT ct.codigodocliente,
       cli.nome,
       translate(cli.cpf_cnpj::text, '.-/'::text, ''::text) as cpf,
       ct.contrato,
       array_agg(split_part(pr.nomedaprogramacao, '[', 1)) as "programacao_ok"
FROM contratos ct
join clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
JOIN cont_prog cp ON cp.cidade = ct.cidade AND cp.contrato = ct.contrato
JOIN programacao pr ON pr.codigodaprogramacao = cp.protabelaprecos AND pr.codcidade = cp.cidade
GROUP BY ct.codigodocliente, cli.nome, cpf, ct.contrato

--EXERCÍCIO 3
/*3) Trazer programacao e programação play
	nome da cidade,
        codprogramacao,
        nome programacao,
        lista de programacoes play em array - nomes*/

SELECT cid.nomedacidade,
       pr.codigodaprogramacao,
       pr.nomedaprogramacao,
       array_agg(split_part(pp.nome, '[', 1)) as programacao_play
FROM programacao pr
join cidade cid on cid.codigodacidade = pr.codcidade
join programacaopacotesplay ppp on ppp.codigoprogramacao = pr.codigodaprogramacao and ppp.codigocidadeprogramacao = pr.codcidade
join programacaoplay pp on pp.codigoprogramacaoplay = ppp.codprogramacaoplay 
GROUP BY cid.nomedacidade,
         pr.codigodaprogramacao,
         pr.nomedaprogramacao

--EXERCÍCIO 4
/*4) Trazer total de contratos por programacao
        cidade,
        codigodaprogramacao,
        nome da programacao,
        total de contratos*/

SELECT cid.nomedacidade, pr.codigodaprogramacao, pr.nomedaprogramacao, count(ct.contrato) as qtde
FROM contratos ct
JOIN cont_prog cp ON cp.cidade = ct.cidade and cp.contrato = ct.contrato and cp.codempresa = ct.codempresa
JOIN programacao pr ON pr.codigodaprogramacao = cp.protabelaprecos AND pr.codcidade = cp.cidade
join cidade cid on cid.codigodacidade = pr.codcidade
GROUP BY cid.nomedacidade, pr.codigodaprogramacao, pr.nomedaprogramacao

--------------------------------------------------------------------------------------------------------------------------------------------

-- ERRO FISCO 218 E 223
-- Tirar a NF do valor positivo de igual valor do negativo
 update movimfinanceiro m set numnf = null, periodo=null, serienf=null, classificacao=null where id in (
  select m1.id
  from movimfinanceiro m 
  JOIN movimfinanceiro m1 ON m1.cidade = m.cidade AND m1.assinante = m1.assinante AND m.periodo::text = m1.periodo::text AND m.serienf::text = m1.serienf::text 
  AND m.numnf = m1.numnf AND m.codempresapacote = m1.codempresapacote and m1.valoros * -1 = m.valoros
  where m.valoros < 0 and m.numnf is not null
);

-- Tira as NF dos valores negativos
update movimfinanceiro m set numnf = null, periodo=null, serienf=null, classificacao=null where id in (
  select m.id
  from movimfinanceiro m where m.valoros < 0 and m.numnf is not null
);


select * from (
  select n.id, n.totalnota, sum(m.valoros) as totalitens
  from nfviaunica n 
  JOIN movimfinanceiro m ON m.cidade = n.codcidade AND m.assinante =
           n.codassinante AND m.periodo::text = n.periodo::text AND m.serienf::text = n.serienf::text AND m.numnf = n.numnf AND m.codempresapacote = n.codempresa
  where n.d_dataemissao between '2022-05-01' and '2021-05-31'
  group by n.id, n.totalnota
) as t
where t.totalnota <> t.totalitens

--------------------------------------------------------------------------------------------------------------------------------------------

-- RELATÓRIO ENVIOS RÉGUA DE COBRANÇA - CONEXÃO
select a.id,
       a.criadoem::date as "Data Operação",
       a.criadoem::time as "Hora Operação",
       CASE WHEN a.tipooperacao = 0 THEN 'E-mail' ELSE 'SMS' END AS "Tipo_Operacao",
       CASE WHEN a.tipooperacao = 0 THEN a.email ELSE a.celular END AS "Destino",
       rg.descricao,
       a.textoerro,
       a.situacao
from reguacobranca.historicoenviosregua a
join reguacobranca.regra rg on rg.id = a.regraid
where a.criadoem::date BETWEEN '2022-06-14' and '2022-06-14'
order by a.criadoem

--------------------------------------------------------------------------------------------------------------------------------------------

-- RELATÓRIO HISTÓRICOS GERAIS - SISTEMA - CONEXÃO
with
  s as (
      select hg.id, 
          CASE WHEN hg.d_datafechamento IS NOT NULL THEN 1
               WHEN hg.d_datafechamento IS NULL AND hpai.d_datafechamento IS NOT NULL THEN  1 
               ELSE 2
          END AS status 
      from historicogeral hg
      LEFT JOIN historicogeral hpai ON hpai.controle = hg.historicopai 
)
select distinct ct.contrato, ci.nomedacidade, cli.codigocliente, cli.nome, cli.cpf_cnpj, hg.controle, hg.atendente, hg.d_datacadastro, 
hg.t_horacadastro, hg.d_datafechamento, hg.t_horafechamento,
CASE WHEN hg.d_datafechamento IS NOT NULL THEN (((hg.d_datafechamento || ' '::text) || hg.t_horafechamento)::timestamp without time zone) -(((
    hg.d_data || ' '::text) || hg.t_hora)::timestamp without time zone)
  WHEN hg.d_datafechamento IS NULL AND hpai.d_datafechamento IS NOT NULL THEN (((hpai.d_datafechamento || ' '::text) ||
    hpai.t_horafechamento)::timestamp without time zone) -(((hpai.d_data || ' '::text) || hpai.t_hora)::timestamp without time zone)
  ELSE NULL::interval
END AS tempo_atendimento, 
  translate(g.descricao,'.-;:,',',') as grupo,
  translate(a.descricao ,'.-:;,',',') as assunto,
  func_retornatelefones(ct.cidade, ct.codigodocliente) as telefones, v.descricaosituacao, 
  case when  s.status = 1 then 'fechado' else 'aberto' end as status,
  e.razaosocial,
  array_to_string(ARRAY(
      select distinct c.descricao 
      from contratos ct
      join carteira c on c.codigo=ct.codcarteira
      where ct.cidade=cli.cidade and ct.codigodocliente=cli.codigocliente
  ), ','::text) AS carteiras, 
  case when hg.historicopai is null then 'Principal' else 'Andamento' end as tipo_historico,
  ct.id as id_contrato,
  t.descricao as situacao_assunto
from historicogeral hg 
left join contratos ct on ct.cidade = hg.codigocidade and ct.codempresa = hg.codempresa and ct.contrato = hg.codcontrato
join clientes cli on cli.cidade=hg.codigocidade and cli.codigocliente=hg.assinante
join cidade ci on ci.codigodacidade=hg.codigocidade
left join empresas e on e.codcidade = hg.codigocidade and e.codempresa = hg.codempresa
LEFT JOIN historicogeral hpai ON hpai.controle = hg.historicopai
JOIN assuntohistorico a ON a.codigogrupo = hg.grupoassunto AND a.codigoassunto = hg.assunto
JOIN grupohistorico g ON g.codigo = hg.grupoassunto
LEFT JOIN usuariosdohistorico u ON u.controlehistorico = hg.controle
LEFT JOIN hwusers hu ON lower(hu.login::text) = lower(u.usuario::text)
LEFT JOIN hwgroups hgr ON hgr.id = hu.groupid
LEFT JOIN hwusers hua ON lower(hua.login::text) = lower(hg.atendente::text )
LEFT JOIN hwgroups hga ON hga.id = hua.groupid
LEFT JOIN tiposituacaohistorico t ON t.codigo = hg.codigotiposituacao
left join vis_situacaocontrato_descricao v on v.situacao=ct.situacao 
join s on s.id = hg.id

where hg.d_datacadastro = '2022-05-14' and hua.id is null

--------------------------------------------------------------------------------------------------------------------------------------------

-- RELATÓRIO DESCONTOS APLICADOS MATCHA

CIDADE;COD_ASSINANTE;NOME;SITUAÇÃO_DESCONTO;DESCRIÇÃO_DESCONTO;NOME_DESCONTO;VALOR_DESCONTO;MES_REFERENCIA

select c.nomedacidade,
       m.assinante,
       cl.nome,
       case 
       when m.numfatura is null then 'Pendência'
       else m.numfatura::text end as situacao_desconto, 
       m.observacao,
       case 
       when p.id is not null then p.nomedaprogramacao 
       else l.descricaodoserv_lanc
       end as desconto_aplicado,
       m.valoros,
       m.d_mesreferencia
     
from movimfinanceiro m
     join public.cidade c on c.codigodacidade = m.cidade
     join public.clientes cl on cl.cidade = m.cidade and cl.codigocliente =
       m.assinante
      left join lanceservicos l on l.codigodoserv_lanc = m.lanc_servico
      left join programacao p on p.codcidade = m.cidade and p.codigodaprogramacao = m.numerodaprogramacao

WHERE m.observacao ilike '%DESCONTO%'
order by c.nomedacidade

--------------------------------------------------------------------------------------------------------------------------------------------

-- RELATÓRIO RECEBIMENTO GERAL - CONEXÃO
SELECT distinct cid . nomedacidade AS cidade,
         cli.codigocliente,
         cli.nome,
         cli.cpf_cnpj,
         CASE
           WHEN length(translate(cli.cpf_cnpj::text, '.-/ '::text, ''::text)) =
             11 THEN 'PF'::text
           ELSE 'PJ'::text
         END AS tipocliente,
         ct.contrato,
         ct.id AS idcontrato,
         CASE
           WHEN dr.reparcelamento = 1 THEN 'Reparcelado'::text
           WHEN dr.boletoequipamento = 1 THEN 'Equipamento'::text
           WHEN dr.situacao = 1 THEN 'Cancelado'::text
           ELSE 'Normal'::text
         END AS tipo,
         dr.numerodocumento,
         dr.nossonumerobanco,
         dr.d_datavencimento AS datavencimento,
         dr.valordocumento,
         dr.valorjuros,
         dr.valormulta,
         dr.valordesconto,
         dr.d_datapagamento AS datapagamento,
         dr.d_dataliquidacao AS dataliquidacao,
         dr.valorpago,
         CASE
           WHEN dr.nomedoarquivoquebaixou IS NOT NULL AND (dr.tipopagamento =
             ANY (ARRAY [ 1, 3 ])) THEN 'Retorno Bancário'::text
           WHEN dr.tipopagamento = 1 THEN 'Dinheiro'::text
           WHEN dr.tipopagamento = 2 THEN 'Cheque'::text
           WHEN dr.tipopagamento = 3 THEN 'Banco'::text
           WHEN dr.tipopagamento = 4 THEN 'Cartão de Débito'::text
           WHEN dr.tipopagamento = 5 THEN 'Cartão de Crédito'::text
           ELSE NULL::text
         END AS tipopagamento,
         l.descricao AS localcobranca,
         tc.descricao AS tipocontrato,
         CASE
           WHEN i.numfatura IS NOT NULL THEN 'SIM'::text
           ELSE 'NÃO'::text
         END AS temnf,
         cid.codigo_regional,
         c.codigo AS cod_unificadora,
         c.descricao AS empresa,
         case 
         	when ct.gerarcobranca = 0 then 'Acumular por Empresa'
            when ct.gerarcobranca = 1 then 'Somente do Contrato'
            when ct.gerarcobranca = 2 then 'Acumulado por Cliente'
         end as "gerar_cobranca" 
   
  FROM docreceber dr
       JOIN cidade cid ON cid . codigodacidade = dr.codigodacidade
       JOIN clientes cli ON cli.cidade = dr.codigodacidade AND cli.codigocliente = dr.cliente
       JOIN regional r ON r.codigo = cid . codigo_regional
       JOIN localcobranca l ON l.codigo = dr.localcobranca
       JOIN movimfinanceiro m ON m.numfatura = dr.fatura
       JOIN contratos ct ON ct.cidade = m.cidade AND ct.codempresa = m.codempresa AND ct.contrato = m.contrato
       JOIN carteira c ON c.codigo = ct.codcarteira
       LEFT JOIN tiposcontrato tc ON tc.codigo = ct.tipodocontrato
       LEFT JOIN itensnf i ON i.numfatura = dr.fatura
       JOIN empresas e ON e.codcidade = ct.cidade AND e.codempresa =
         ct.codempresa
       JOIN unificadora u ON u.codigo = e.codunificadora
WHERE dr.situacao = 0 and dr.d_datapagamento = '2022-05-20'

--------------------------------------------------------------------------------------------------------------------------------------------

-- TÍTULOS NÃO BAIXADOS
select dr.codigodacidade, dr.id, dr.cliente, dr.d_datavencimento, dr.d_datapagamento, dr.nossonumerobanco, bx.nossonumerobanco, bx.*
    from padroesbancarios.boletosretorno bx
    join public.docreceber dr on dr.nossonumero = bx.nossonumerobanco::bigint
    where bx.nomearquivo ilike '%COBST_QNPV_02_210622P_MOV%'
    and bx.ocorrencia_codigo = '06' and dr.d_datapagamento is null
    order by bx.nossonumerobanco

--------------------------------------------------------------------------------------------------------------------------------------------

-- INCLUI USUÁRIO NO ARMAZÉM
update armazem set usuariosarmazem = usuariosarmazem || ',TESTE'
where codigo in (13)

--------------------------------------------------------------------------------------------------------------------------------------------

--RELATÓRIO RETENÇÃO DE CLIENTES CÁSSIA
with
ativ as (
select w.cidade,
       w.codempresa,
       w.contrato,
       w.numos,
       sum(w.valorpacote_ativacao) as valorpacote_ativacao,
       sum(w.valor_desconto_ativacao) as valor_desconto_ativacao,
       w.pacote_ativacao
from (
       select t.cidade,
              t.codempresa,
              t.contrato,
              t.numos,
              t.valorpacote_ativacao,
              t.valor_desconto_ativacao,
              array_to_string(ARRAY
              (
                select distinct p.nomedaprogramacao
                from variacaodepacotes v
                     join programacao p on p.codcidade = v.cidade and p.codigodaprogramacao = v.pacote
                where v.cidade = t.cidade and
                      v.codempresa = t.codempresa and
                      v.numos = t.numos and
                      v.operacao = 1
              ), ' - ', '') as pacote_ativacao
       from (
              select v.cidade,
                     v.codempresa,
                     v.contrato,
                     v.numos,
                     p.codigodaprogramacao,
                     case
                       when v.operacao = 1 then p.nomedaprogramacao
                       else ''
                     end as pacote_ativacao,
                     case
                       when v.operacao = 1 then v.valorpacote
                       else 0
                     end as valorpacote_ativacao,
                     case
                       when v.operacao = 1 then public.func_calculavaloraditivos_v2(v.cidade, v.codempresa, v.contrato, p.tipoponto::integer,
                         p.tipoprogramacao::integer, v.valorpacote -(v.valorpacote * tc.desconto / 100), '2022-01-01'::date, '2022-06-23'::date,
                         v.pacote::integer)
                       else 0
                     end as valor_desconto_ativacao
              from variacaodepacotes v
                   join programacao p on p.codcidade = v.cidade and p.codigodaprogramacao = v.pacote
                   join contratos ct on ct.cidade = v.cidade and ct.codempresa = v.codempresa and ct.contrato = v.contrato
                   join tiposcontrato tc on tc.codigo = ct.tipodocontrato
              where v.operacao = 1 and
                    v.d_data between '2022-01-01'::date and
                    '2022-06-23'::date
            ) as t
     ) as w
group by w.cidade,
         w.codempresa,
         w.contrato,
         w.numos,
         w.pacote_ativacao),
dest as (
select w.cidade,
       w.codempresa,
       w.contrato,
       w.numos,
       sum(w.valorpacote_desativacao) as valorpacote_desativacao,
       sum(w.valor_desconto_desativacao) as valor_desconto_desativacao,
       w.pacote_desativacao
from (
       select t.cidade,
              t.codempresa,
              t.contrato,
              t.numos,
              t.valorpacote_desativacao,
              t.valor_desconto_desativacao,
              array_to_string(ARRAY
              (
                select distinct p.nomedaprogramacao
                from variacaodepacotes v
                     join programacao p on p.codcidade = v.cidade and p.codigodaprogramacao = v.pacote
                where v.cidade = t.cidade and
                      v.codempresa = t.codempresa and
                      v.numos = t.numos and
                      v.operacao = 2
              ), ' - ', '') as pacote_desativacao
       from (
              select v.cidade,
                     v.codempresa,
                     v.contrato,
                     v.numos,
                     case
                       when v.operacao = 2 then p.nomedaprogramacao
                       else ''
                     end as pacote_desativacao,
                     case
                       when v.operacao = 2 then v.valorpacote
                       else 0
                     end as valorpacote_desativacao,
                     case
                       when v.operacao = 2 then public.func_calculavaloraditivos_v2(v.cidade, v.codempresa, v.contrato, p.tipoponto::integer,
                         p.tipoprogramacao::integer, v.valorpacote -(v.valorpacote * tc.desconto / 100), '2022-01-01'::date, '2022-06-23'::date,
                         v.pacote::integer)
                       else 0
                     end as valor_desconto_desativacao
              from variacaodepacotes v
                   join programacao p on p.codcidade = v.cidade and p.codigodaprogramacao = v.pacote
                   join contratos ct on ct.cidade = v.cidade and ct.codempresa = v.codempresa and ct.contrato = v.contrato
                   join tiposcontrato tc on tc.codigo = ct.tipodocontrato
              where v.operacao = 2 and
                    v.d_data between '2022-01-01'::date and
                    '2022-06-23'::date
            ) as t
     ) as w
group by w.cidade,
         w.codempresa,
         w.contrato,
         w.numos,
         w.pacote_desativacao),
ordserv as (
select distinct os.cidade,
       os.codempresa,
       os.numos
from public.ordemservico os
     join public.lanceservicos l on l.codigodoserv_lanc = os.codservsolicitado
where os.d_dataexecucao between '2022-01-01'::date and
      '2022-06-23'::date /*and l.baixapontosmarcados = 4*/)
select distinct os.d_dataatendimento,
       os.numos,
       l.descricaodoserv_lanc,
       CASE
         WHEN length(translate(cli.cpf_cnpj::text, ' .,:-//\_+='::text, ''::text)) = 14 THEN 'Pessoa Juridica'::text
         ELSE 'Pessoa Fisica'::text
       END AS tipo_pessoa,
       cli.codigocliente,
       cli.nome,
       case
         when os.situacao = 1 then 'Pendente'
         when os.situacao = 2 then 'Atendimento'
         when os.situacao = 3 then 'Executada'
       End as situacao,
       tab.nome,
       tab.estado,
       '' as "grupo_cadastro",
       ct.contrato,
       '' as "codigo_plano",
       a.pacote_ativacao,
       a.valor_desconto_ativacao as "valor_pacote_ativacao",
       v.descricaosituacao,
       os.atendente,
       '' as "setor",
       '' as "status",
       '' as "ocorrencia",       
       d.pacote_desativacao,
       d.valor_desconto_desativacao as "valor_pacote_desativacao",
       (a.valorpacote_ativacao - d.valorpacote_desativacao) as "valor_desconto_desativacao",
       '' as "comissao",
       '' as "upgrade_plano"
from ordserv oo
     join ordemservico os on os.cidade = oo.cidade and os.codempresa = oo.codempresa and os.numos = oo.numos
     join contratos ct on ct.cidade = os.cidade and ct.codempresa = os.codempresa and ct.contrato = os.codigocontrato
     join cidade cid on cid . codigodacidade = os.cidade
     join regional r on r.codigo = cid . codigo_regional
     JOIN tablocal tab ON tab.codigo = ct.cidade
     join clientes cli on cli.cidade = os.cidade and cli.codigocliente = os.codigoassinante
     join lanceservicos l on l.codigodoserv_lanc = os.codservsolicitado
     left join hwusers h on lower(h.login) = lower(os.atendente)
     left join hwgroups hg on hg.id = h.groupid
     join vis_situacaocontrato_descricao v on v.situacao = ct.situacao
     left join ativ a on a.cidade = oo.cidade and a.codempresa = oo.codempresa and a.numos = oo.numos
     left join dest d on d.cidade = a.cidade and d.codempresa = a.codempresa and d.numos = a.numos
where os.d_dataexecucao between '2022-01-01' and '2022-06-23' /*and l.baixapontosmarcados = 4*/ and
 l.codigodoserv_lanc IN (111181, 111191, 111201, 111161, 111151, 111171)
 
--------------------------------------------------------------------------------------------------------------------------------------------

--CRIAR FUNÇÃO CURSO POSTGRES
create function soma_tres(
v1 integer,
v2 integer,
v3 integer)
RETURNS integer AS
$body$
DECLARE
resultado integer;
begin
resultado := v1 + v2 +v3;
return resultado;
end;
$body$
language 'plpgsal';

select * from soma_tres(1,2,3);

--------------------------------------------------------------------------------------------------------------------------------------------

--CRIRAR FUNÇÃO CURSO POSTGRES
CREATE OR REPLACE FUNCTION public.tri_mudamae_assinante1(
)
RETURNS trigger AS
$body$
declare
begin

	new.nomemae := 'Etevalda';
    
 return new;
end;
$body$
LANGUAGE 'plpgsql';

CREATE TRIGGER tri_mudamae_assinante1
	BEFORE UPDATE
    ON public.clientes
    
FOR EACH ROW
	EXECUTE PROCEDURE public.tri_mudamae_assinante1();

select cl.nomemae from clientes cl where id = 13

update clientes set nomemae = 'Josefina' where id = 13

ALTER TABLE public.clientes
	ENABLE TRIGGER tri_mudamae_assinante;
	
--------------------------------------------------------------------------------------------------------------------------------------------

-- EXERCÍCIOS CURSO POSTGRES
/*1 - Trazer cliente, endereço residencial e profissao
	cidade
	nome cliente
	nome profissao
	nome rua
	tipo endereco
	numero*/

select cid.nomedacidade,
       cl.nome,
       pr.descricao as profissao,
       ed.nomelogradouro,
       ed.tipodologradouro,
       cl.numeroresidencial
	from clientes cl
    JOIN cidade cid ON cid.codigodacidade = cl.cidade
    JOIN enderecos ed ON ed.codigodacidade = cl.cidade and ed.codigodologradouro = cl.enderecoresidencial
    JOIN profissoes pr ON pr.codigo = cl.profissao

limit 10000


/*2 - Trazer qtd de contratos por empresa
	nome empresa
	cidade
	qtd*/

select emp.razaosocial,
       cid.nomedacidade,
       count(ct.contrato) as qtd_contratos
from contratos ct
JOIN cidade cid ON cid.codigodacidade = ct.cidade
JOIN empresas emp ON emp.codempresa = ct.codempresa and emp.codcidade = ct.cidade
GROUP BY emp.razaosocial, cid.nomedacidade


/*3 - Trazer ordens do mes de junho e equipe que executou
	cidade
	numos
	nome servico	
	nome equipe*/
    
select cid.nomedacidade,
       os.numos,
       ls.descricaodoserv_lanc,
       e.nomedaequipe
from ordemservico os
join lanceservicos ls on ls.codigodoserv_lanc = os.codservsolicitado
JOIN cidade cid ON cid.codigodacidade = os.cidade
JOIN equipe e ON e.codigocidade = os.cidade AND e.codigodaequipe = os.equipeexecutou

where os.d_databaixa BETWEEN '2022-06-01' and '2022-06-24'


/*4- Trazer pendencias do mes de julho, valores nao faturados
	cidade
	nome assinante
	valor
	obs*/

select cid.nomedacidade,
       cl.nome,
       mf.valoros,
       mf.observacao
from movimfinanceiro mf
JOIN contratos ct ON ct.cidade = mf.cidade and ct.contrato = mf.contrato and ct.codempresa = mf.codempresa
JOIN cidade cid ON cid.codigodacidade = mf.cidade
JOIN clientes cl ON cl.codigocliente = ct.codigodocliente and cl.cidade = ct.cidade

where mf.d_mesreferencia = '2022-07-01' and mf.numfatura is null

--------------------------------------------------------------------------------------------------------------------------------------------

-- ANTIGAS MASTER:
1nt3rf0cusm4st3r
------------------------------------------------------------------------------
Senha arquivo config. TOPSCAN = x7w3y6
============================================================================
Relatorio Movidesk
powerbi@interfocus.com.br
Pow3rBI201902

--------------------------------------------------------------------------------------------------------------------------------------------

-- RELATÓRIO MOVIMENTAÇÃO DE PLANOS CONEXÃO
with 
  ativ as (
    select car.descricao as empresa, c.id, c.tipo, c.codcidade, to_ascii(upper(c.nomecidade)) as nomecidade, c.ufcidade, 
    case when c.nomevendedor is null then os.atendente else upper(to_ascii(btrim(c.nomevendedor))) end as nomevendedor, 
    case when os.usuario_baixa is not null then os.usuario_baixa else 
      case when position('Ordem de Serviço baixado por: ' in os.observacoes) > 1 then substr(os.observacoes,position('Ordem de Serviço baixado por: ' in os.observacoes)+30,length(os.observacoes)) else
      substr(os.observacoes,position('Nome Executante: ' in os.observacoes)+17,position('Nome Executante: ' in os.observacoes)+37) end 
      end as usuario_baixou, c.dataexecucao, c.numos, c.nomeservico, c.codassinante, c.nomeassinante, c.numcontrato, c.codequipevenda, c.codpacote, c.nomepacote,
      c.valorpacote, c.acaopacote, ev.descricao as equipe_venda, u.descricao as unificadora,
      extract(year from c.dataexecucao) as ano, to_char(c.dataexecucao,'TMMonth') as nome_mes,
      substr(to_char(c.dataexecucao,'TMMonth'),1,3)|| '/'||extract(year from c.dataexecucao) as mes_ano, 
      ct.id as idcontrato, ct.codcarteira, row_number() over (partition by c.codcidade,c.codassinante,c.numos order by c.codcidade,c.codassinante,c.numos) as seq,
      func_calculavaloraditivos_v2(ct.cidade,ct.codempresa,ct.contrato,pr.tipoponto,pr.tipoprogramacao,c.valorpacote,
         to_char(c.dataexecucao,'YYYY-MM-01')::date,(to_char(c.dataexecucao,'YYYY-MM-01')::date + '1 mon'::interval)::date-1 ,pr.codigodaprogramacao) as valor_desconto
    from gerencial.comissaodetalhada c
    join public.cidade cid on cid.codigodacidade = c.codcidade
    join public.ordemservico os on os.cidade = c.codcidade and os.numos = c.numos and os.codigocontrato = c.numcontrato
    join public.contratos ct on ct.cidade = os.cidade and ct.codempresa = os.codempresa and ct.contrato = os.codigocontrato
    left join public.carteira car on car.codigo=ct.codcarteira
    join public.programacao pr on pr.codcidade = c.codcidade and pr.codigodaprogramacao = c.codpacote
    left join public.equipesdevenda ev on ev.cidade = c.codcidade and ev.codigo = c.codequipevenda
    left join public.unificadora u on u.codigo = ev.codigounificadora
    left join public.vendedores v on v.cidadeondetrabalha=os.cidade and v.codigo=os.codvendedor and v.equipevenda=os.codequipevenda
    left join public.vendedores vc on vc.cidadeondetrabalha=ct.cidade and vc.codigo=ct.vendedor and vc.equipevenda=ct.equipedevenda
    where os.codservsolicitado not in (11,1431,1481,1831,1611) and c.acaopacote = 'Ativado' and car.codigo = 41
  ),
  desat as (
    select car.descricao as empresa, c.id, c.tipo, c.codcidade, to_ascii(upper(c.nomecidade)) as nomecidade, c.ufcidade, 
    case when c.nomevendedor is null then os.atendente else upper(to_ascii(btrim(c.nomevendedor))) end as nomevendedor, 
    case when os.usuario_baixa is not null then os.usuario_baixa else 
      case when position('Ordem de Serviço baixado por: ' in os.observacoes) > 1 then substr(os.observacoes,position('Ordem de Serviço baixado por: ' in os.observacoes)+30,length(os.observacoes)) else
      substr(os.observacoes,position('Nome Executante: ' in os.observacoes)+17,position('Nome Executante: ' in os.observacoes)+37) end 
      end as usuario_baixou,
      c.dataexecucao,  c.numos, c.nomeservico, c.codassinante, c.nomeassinante, c.numcontrato, c.codequipevenda, c.codpacote, c.nomepacote,
      (c.valorpacote * -1) as valorpacote, c.acaopacote, ev.descricao as equipe_venda, u.descricao as unificadora,
      extract(year from c.dataexecucao) as ano, to_char(c.dataexecucao,'TMMonth') as nome_mes,
      substr(to_char(c.dataexecucao,'TMMonth'),1,3)|| '/'||extract(year from c.dataexecucao) as mes_ano, 
      ct.id as idcontrato, ct.codcarteira, row_number() over (partition by c.codcidade,c.codassinante,c.numos order by c.codcidade,c.codassinante,c.numos) as seq,
      func_calculavaloraditivos_v2(ct.cidade,ct.codempresa,ct.contrato,pr.tipoponto,pr.tipoprogramacao,(c.valorpacote*-1),
         to_char(c.dataexecucao,'YYYY-MM-01')::date,(to_char(c.dataexecucao,'YYYY-MM-01')::date + '1 mon'::interval)::date-1 ,pr.codigodaprogramacao) as valor_desconto
    from gerencial.comissaodetalhada c
    join public.cidade cid on cid.codigodacidade = c.codcidade
    join public.ordemservico os on os.cidade = c.codcidade and os.numos = c.numos and os.codigocontrato = c.numcontrato
    join public.contratos ct on ct.cidade = os.cidade and ct.codempresa = os.codempresa and ct.contrato = os.codigocontrato
    left join public.carteira car on car.codigo=ct.codcarteira
    join public.programacao pr on pr.codcidade = c.codcidade and pr.codigodaprogramacao = c.codpacote
    left join public.equipesdevenda ev on ev.cidade = c.codcidade and ev.codigo = c.codequipevenda
    left join public.unificadora u on u.codigo = ev.codigounificadora
    left join public.vendedores v on v.cidadeondetrabalha=os.cidade and v.codigo=os.codvendedor and v.equipevenda=os.codequipevenda
    left join public.vendedores vc on vc.cidadeondetrabalha=ct.cidade and vc.codigo=ct.vendedor and vc.equipevenda=ct.equipedevenda
    where os.codservsolicitado not in (11,1431,1481,1831,1611,1451,2111,2081,2091,2101,1591,1531,1581,1661,1371,1351) and c.acaopacote = 'Desativado' and car.codigo = 41
  )
select COALESCE(a.empresa,d.empresa), COALESCE(a.id,d.id), COALESCE(a.tipo,d.tipo), COALESCE(a.codcidade,d.codcidade), 
COALESCE(a.nomecidade,d.nomecidade), COALESCE(a.ufcidade,d.ufcidade), COALESCE(a.nomevendedor,d.nomevendedor), 
translate(COALESCE(a.usuario_baixou,d.usuario_baixou),E'
',''), COALESCE(a.dataexecucao,d.dataexecucao), COALESCE(a.numos,d.numos), 
COALESCE(a.nomeservico,d.nomeservico),COALESCE(a.codassinante,d.codassinante), COALESCE(a.nomeassinante,d.nomeassinante), 
COALESCE(a.numcontrato,d.numcontrato), 
d.acaopacote, d.codpacote, d.nomepacote, d.valorpacote, d.valor_desconto,
COALESCE(a.codequipevenda,d.codequipevenda), COALESCE(a.acaopacote,d.acaopacote), COALESCE(a.codpacote,d.codpacote), 
COALESCE(a.nomepacote,d.nomepacote), COALESCE(a.valorpacote,d.valorpacote), COALESCE(a.valor_desconto,d.valor_desconto), 
a.valorpacote - d.valorpacote as saldo, a.valor_desconto - d.valor_desconto as saldo_desconto,
COALESCE(a.equipe_venda,d.equipe_venda), COALESCE(a.unificadora,d.unificadora), COALESCE(a.ano,d.ano), COALESCE(a.nome_mes,d.nome_mes), 
COALESCE(a.mes_ano,d.mes_ano), COALESCE(a.idcontrato,d.idcontrato)
from ativ a
right join desat d on d.codcidade=a.codcidade and d.codassinante=a.codassinante and d.numos=a.numos and d.seq=a.seq

--------------------------------------------------------------------------------------------------------------------------------------------

--VIEW RELATÓRIO CONEXÃO
CREATE OR REPLACE VIEW regrasoperacao.vis_movimentacao_pacotes_conexao_cnx(
    empresa,
    id,
    tipo,
    cod_cidade,
    cidade,
    uf,
    vendedor,
    usuario_baixa,
    data_execucao,
    numos,
    nome_servico,
    cod_assinante,
    nome_assinante,
    cod_contrato,
    acao_pacote_des,
    cod_pacote_des,
    nomepacote_des,
    valorpacote_des,
    valor_desconto_des,
    cod_equipe_venda,
    acao_pacote,
    cod_pacote,
    pacote,
    valor_pacote,
    valor_pacote_desconto,
    saldo,
    saldo_desconto,
    equipe_venda,
    unificadora,
    ano,
    nome_mes,
    mes_ano,
    id_contrato,
    cod_carteira,
    nome_carteira)
AS
WITH ativ AS(
  SELECT car.descricao AS empresa,
         c.id,
         c.tipo,
         c.codcidade,
         to_ascii(upper(c.nomecidade)) AS nomecidade,
         c.ufcidade,
         CASE
           WHEN c.nomevendedor IS NULL THEN os.atendente::text
           ELSE upper(to_ascii(btrim(c.nomevendedor)))
         END AS nomevendedor,
         CASE
           WHEN os.usuario_baixa IS NOT NULL THEN os.usuario_baixa::text
           ELSE CASE
                  WHEN "position"(os.observacoes::text, 'Ordem de Serviço baixado por: '::text) > 1 THEN substr(os.observacoes::text, "position"(
                    os.observacoes::text, 'Ordem de Serviço baixado por: '::text) + 30, length(os.observacoes::text))
                  ELSE substr(os.observacoes::text, "position"(os.observacoes::text, 'Nome Executante: '::text) + 17, "position"(os.observacoes::text,
                    'Nome Executante: '::text) + 37)
                END
         END AS usuario_baixou,
         c.dataexecucao,
         c.numos,
         c.nomeservico,
         c.codassinante,
         c.nomeassinante,
         c.numcontrato,
         c.codequipevenda,
         c.codpacote,
         c.nomepacote,
         c.valorpacote,
         c.acaopacote,
         ev.descricao AS equipe_venda,
         u.descricao AS unificadora,
         date_part('year'::text, c.dataexecucao) AS ano,
         to_char(c.dataexecucao::timestamp with time zone, 'TMMonth'::text) AS nome_mes,
         (substr(to_char(c.dataexecucao::timestamp with time zone, 'TMMonth'::text), 1, 3) || '/'::text) || date_part('year'::text, c.dataexecucao) AS
           mes_ano,
         ct.id AS idcontrato,
         ct.codcarteira,
         row_number() OVER(PARTITION BY c.codcidade, c.codassinante, c.numos
  ORDER BY c.codcidade, c.codassinante, c.numos) AS seq,
           func_calculavaloraditivos_v2(ct.cidade, ct.codempresa, ct.contrato, pr.tipoponto::integer, pr.tipoprogramacao::integer, c.valorpacote,
             to_char(c.dataexecucao::timestamp with time zone, 'YYYY-MM-01'::text)::date, (to_char(c.dataexecucao::timestamp with time zone,
             'YYYY-MM-01'::text)::date + '1 mon'::interval)::date - 1, pr.codigodaprogramacao) AS valor_desconto,
           car.codigo AS cod_carteira,
           car.descricao AS nome_carteira
  FROM gerencial.comissaodetalhada c
       JOIN cidade cid ON cid . codigodacidade = c.codcidade
       JOIN ordemservico os ON os.cidade = c.codcidade AND os.numos = c.numos AND os.codigocontrato = c.numcontrato
       JOIN contratos ct ON ct.cidade = os.cidade AND ct.codempresa = os.codempresa AND ct.contrato = os.codigocontrato
       JOIN carteira car ON car.codigo = ct.codcarteira
       JOIN programacao pr ON pr.codcidade = c.codcidade AND pr.codigodaprogramacao = c.codpacote
       LEFT JOIN equipesdevenda ev ON ev.cidade = c.codcidade AND ev.codigo = c.codequipevenda
       LEFT JOIN unificadora u ON u.codigo = ev.codigounificadora
       LEFT JOIN vendedores v ON v.cidadeondetrabalha = os.cidade AND v.codigo = os.codvendedor AND v.equipevenda = os.codequipevenda
       LEFT JOIN vendedores vc ON vc.cidadeondetrabalha = ct.cidade AND vc.codigo = ct.vendedor AND vc.equipevenda = ct.equipedevenda
  WHERE (os.codservsolicitado <> ALL (ARRAY [ 11, 1431, 1481, 1831, 1611 ])) AND
        c.acaopacote = 'Ativado'::text AND
        car.codigo = 41), desat AS (
         SELECT car.descricao AS empresa,
                c.id,
                c.tipo,
                c.codcidade,
                to_ascii(upper(c.nomecidade)) AS nomecidade,
                c.ufcidade,
                CASE
                  WHEN c.nomevendedor IS NULL THEN os.atendente::text
                  ELSE upper(to_ascii(btrim(c.nomevendedor)))
                END AS nomevendedor,
                CASE
                  WHEN os.usuario_baixa IS NOT NULL THEN os.usuario_baixa::text
                  ELSE CASE
                         WHEN "position"(os.observacoes::text, 'Ordem de Serviço baixado por: '::text) > 1 THEN substr(os.observacoes::text,
                           "position"(os.observacoes::text, 'Ordem de Serviço baixado por: '::text) + 30, length(os.observacoes::text))
                         ELSE substr(os.observacoes::text, "position"(os.observacoes::text, 'Nome Executante: '::text) + 17, "position"(os.observacoes
                           ::text, 'Nome Executante: '::text) + 37)
                       END
                END AS usuario_baixou,
                c.dataexecucao,
                c.numos,
                c.nomeservico,
                c.codassinante,
                c.nomeassinante,
                c.numcontrato,
                c.codequipevenda,
                c.codpacote,
                c.nomepacote,
                c.valorpacote * '-1'::integer::numeric AS valorpacote,
                c.acaopacote,
                ev.descricao AS equipe_venda,
                u.descricao AS unificadora,
                date_part('year'::text, c.dataexecucao) AS ano,
                to_char(c.dataexecucao::timestamp with time zone, 'TMMonth'::text) AS nome_mes,
                (substr(to_char(c.dataexecucao::timestamp with time zone, 'TMMonth'::text), 1, 3) || '/'::text) || date_part('year'::text,
                  c.dataexecucao) AS mes_ano,
                ct.id AS idcontrato,
                ct.codcarteira,
                row_number() OVER(PARTITION BY c.codcidade, c.codassinante, c.numos
         ORDER BY c.codcidade, c.codassinante, c.numos) AS seq,
                  func_calculavaloraditivos_v2(ct.cidade, ct.codempresa, ct.contrato, pr.tipoponto::integer, pr.tipoprogramacao::integer,
                    c.valorpacote * '-1'::integer::numeric, to_char(c.dataexecucao::timestamp with time zone, 'YYYY-MM-01'::text)::date, (to_char(
                    c.dataexecucao::timestamp with time zone, 'YYYY-MM-01'::text)::date + '1 mon'::interval)::date - 1, pr.codigodaprogramacao) AS
                    valor_desconto,
                  car.codigo AS cod_carteira,
                  car.descricao AS nome_carteira
         FROM gerencial.comissaodetalhada c
              JOIN cidade cid ON cid . codigodacidade = c.codcidade
              JOIN ordemservico os ON os.cidade = c.codcidade AND os.numos = c.numos AND os.codigocontrato = c.numcontrato
              JOIN contratos ct ON ct.cidade = os.cidade AND ct.codempresa = os.codempresa AND ct.contrato = os.codigocontrato
              JOIN carteira car ON car.codigo = ct.codcarteira
              JOIN programacao pr ON pr.codcidade = c.codcidade AND pr.codigodaprogramacao = c.codpacote
              LEFT JOIN equipesdevenda ev ON ev.cidade = c.codcidade AND ev.codigo = c.codequipevenda
              LEFT JOIN unificadora u ON u.codigo = ev.codigounificadora
              LEFT JOIN vendedores v ON v.cidadeondetrabalha = os.cidade AND v.codigo = os.codvendedor AND v.equipevenda = os.codequipevenda
              LEFT JOIN vendedores vc ON vc.cidadeondetrabalha = ct.cidade AND vc.codigo = ct.vendedor AND vc.equipevenda = ct.equipedevenda
         WHERE (os.codservsolicitado <> ALL (ARRAY [ 11, 1431, 1481, 1831, 1611, 1451, 2111, 2081, 2091, 2101, 1591, 1531, 1581, 1661, 1371, 1351 ]))
  AND
               c.acaopacote = 'Desativado'::text AND
               car.codigo = 41)
 SELECT COALESCE(a.empresa, d.empresa) AS empresa,
        COALESCE(a.id, d.id) AS id,
        COALESCE(a.tipo, d.tipo) AS tipo,
        COALESCE(a.codcidade, d.codcidade) AS cod_cidade,
        COALESCE(a.nomecidade, d.nomecidade) AS cidade,
        COALESCE(a.ufcidade, d.ufcidade) AS uf,
        COALESCE(a.nomevendedor, d.nomevendedor) AS vendedor,
        translate(COALESCE(a.usuario_baixou, d.usuario_baixou), '
'::text, ''::text) AS usuario_baixa,
        COALESCE(a.dataexecucao, d.dataexecucao) AS data_execucao,
        COALESCE(a.numos, d.numos) AS numos,
        COALESCE(a.nomeservico, d.nomeservico) AS nome_servico,
        COALESCE(a.codassinante, d.codassinante) AS cod_assinante,
        COALESCE(a.nomeassinante, d.nomeassinante) AS nome_assinante,
        COALESCE(a.numcontrato, d.numcontrato) AS cod_contrato,
        d.acaopacote AS acao_pacote_des,
        d.codpacote AS cod_pacote_des,
        d.nomepacote AS nomepacote_des,
        d.valorpacote AS valorpacote_des,
        d.valor_desconto AS valor_desconto_des,
        COALESCE(a.codequipevenda, d.codequipevenda) AS cod_equipe_venda,
        COALESCE(a.acaopacote, d.acaopacote) AS acao_pacote,
        COALESCE(a.codpacote, d.codpacote) AS cod_pacote,
        COALESCE(a.nomepacote, d.nomepacote) AS pacote,
        COALESCE(a.valorpacote, d.valorpacote) AS valor_pacote,
        COALESCE(a.valor_desconto, d.valor_desconto) AS valor_pacote_desconto,
        a.valorpacote - d.valorpacote AS saldo,
        a.valor_desconto - d.valor_desconto AS saldo_desconto,
        COALESCE(a.equipe_venda, d.equipe_venda) AS equipe_venda,
        COALESCE(a.unificadora, d.unificadora) AS unificadora,
        COALESCE(a.ano, d.ano) AS ano,
        COALESCE(a.nome_mes, d.nome_mes) AS nome_mes,
        COALESCE(a.mes_ano, d.mes_ano) AS mes_ano,
        COALESCE(a.idcontrato, d.idcontrato) AS id_contrato,
        COALESCE(a.cod_carteira, d.cod_carteira) AS cod_carteira,
        COALESCE(a.nome_carteira, d.nome_carteira) AS nome_carteira
 FROM ativ a
      RIGHT JOIN desat d ON d.codcidade = a.codcidade AND d.codassinante = a.codassinante AND d.numos = a.numos AND d.seq = a.seq;

--------------------------------------------------------------------------------------------------------------------------------------------

--RELATÓRIO ASSINANTE X PROGRAMAÇÃO E TECNOLOGIA_R&R CÁSSIA TICKET 28199
with
a as(SELECT t.idcliente,
t.tipoequipamento,
t.nomeplano
FROM dblink('hostaddr=187.86.112.102 dbname=ins user=postgres password=i745@postgres port=5432'::text, 'select e.idcliente,
case when e.tipoequipamento = 1 then ''CABLE MODEM''
when e.tipoequipamento = 2 then ''CPE DHCP''
when e.tipoequipamento = 3 then ''CPE RADIUS''
when e.tipoequipamento = 4 then ''DECODER''
when e.tipoequipamento = 5 then ''CPE HOT SPOT''
when e.tipoequipamento = 6 then ''EMTA''
when e.tipoequipamento = 7 then ''CPE RADIUS POR DHCP''
when e.tipoequipamento = 8 then ''TELEFONIA''
when e.tipoequipamento = 9 then ''ONU''
end as tipoequipamento,
case when p.nomeplano is not null and p.nomeplano <> '''' then p.nomeplano else ''LINK LÓGICO: ''||pe.linklogico||''- PLANO: ''||pl.nomeplano end as nomeplano
from idhcp.equipamentos e
left join idhcp.planos p on p.id = e.idplano
left join idhcp.planosequipamento pe on pe.idequipamento = e.id
left join idhcp.planos pl on pl.id = pe.idplano
where e.idmotivosderetirada is null and e.tipoequipamento not in (9)
order by e.idcliente'::text) t(idcliente bigint, tipoequipamento character varying, nomeplano character varying)
),
b AS(SELECT t.idcliente,
t.tipoequipamento,
t.nomeplano
FROM dblink('hostaddr=186.194.224.5 dbname=ins user=postgres password=i745@postgres port=5432'::text, 'select e.idcliente,
case when e.tipoequipamento = 1 then ''CABLE MODEM''
when e.tipoequipamento = 2 then ''CPE DHCP''
when e.tipoequipamento = 3 then ''CPE RADIUS''
when e.tipoequipamento = 4 then ''DECODER''
when e.tipoequipamento = 5 then ''CPE HOT SPOT''
when e.tipoequipamento = 6 then ''EMTA''
when e.tipoequipamento = 7 then ''CPE RADIUS POR DHCP''
when e.tipoequipamento = 8 then ''TELEFONIA''
when e.tipoequipamento = 9 then ''ONU''
end as tipoequipamento,
case when p.nomeplano is not null and p.nomeplano <> '''' then p.nomeplano else ''LINK LÓGICO: ''||pe.linklogico||''- PLANO: ''||pl.nomeplano end as nomeplano
from idhcp.equipamentos e
left join idhcp.planos p on p.id = e.idplano
left join idhcp.planosequipamento pe on pe.idequipamento = e.id
left join idhcp.planos pl on pl.id = pe.idplano
where e.idmotivosderetirada is null and e.tipoequipamento not in (9)
order by e.idcliente'::text) t(idcliente bigint, tipoequipamento character varying, nomeplano character varying)
),
c as(SELECT t.idcliente,
t.tipoequipamento,
t.nomeplano
FROM dblink('hostaddr=177.185.176.5 dbname=ins user=postgres password=i745@postgres port=5432'::text, 'select e.idcliente,
case when e.tipoequipamento = 1 then ''CABLE MODEM''
when e.tipoequipamento = 2 then ''CPE DHCP''
when e.tipoequipamento = 3 then ''CPE RADIUS''
when e.tipoequipamento = 4 then ''DECODER''
when e.tipoequipamento = 5 then ''CPE HOT SPOT''
when e.tipoequipamento = 6 then ''EMTA''
when e.tipoequipamento = 7 then ''CPE RADIUS POR DHCP''
when e.tipoequipamento = 8 then ''TELEFONIA''
when e.tipoequipamento = 9 then ''ONU''
end as tipoequipamento,
case when p.nomeplano is not null and p.nomeplano <> '''' then p.nomeplano else ''LINK LÓGICO: ''||pe.linklogico||''- PLANO: ''||pl.nomeplano end as nomeplano
from idhcp.equipamentos e
left join idhcp.planos p on p.id = e.idplano
left join idhcp.planosequipamento pe on pe.idequipamento = e.id
left join idhcp.planos pl on pl.id = pe.idplano
where e.idmotivosderetirada is null and e.tipoequipamento not in (9)
order by e.idcliente'::text) t(idcliente bigint, tipoequipamento character varying, nomeplano character varying)
),
d as(SELECT t.idcliente,
t.tipoequipamento,
t.nomeplano
FROM dblink('hostaddr=189.51.144.21 dbname=ins user=postgres password=i745@postgres port=5432'::text, 'select e.idcliente,
case when e.tipoequipamento = 1 then ''CABLE MODEM''
when e.tipoequipamento = 2 then ''CPE DHCP''
when e.tipoequipamento = 3 then ''CPE RADIUS''
when e.tipoequipamento = 4 then ''DECODER''
when e.tipoequipamento = 5 then ''CPE HOT SPOT''
when e.tipoequipamento = 6 then ''EMTA''
when e.tipoequipamento = 7 then ''CPE RADIUS POR DHCP''
when e.tipoequipamento = 8 then ''TELEFONIA''
when e.tipoequipamento = 9 then ''ONU''
end as tipoequipamento,
case when p.nomeplano is not null and p.nomeplano <> '''' then p.nomeplano else ''LINK LÓGICO: ''||pe.linklogico||''- PLANO: ''||pl.nomeplano end as nomeplano
from idhcp.equipamentos e
left join idhcp.planos p on p.id = e.idplano
left join idhcp.planosequipamento pe on pe.idequipamento = e.id
left join idhcp.planos pl on pl.id = pe.idplano
where e.idmotivosderetirada is null and e.tipoequipamento not in (9)
order by e.idcliente'::text) t(idcliente bigint, tipoequipamento character varying, nomeplano character varying)
),
f as(SELECT t.idcliente,
t.tipoequipamento,
t.nomeplano
FROM dblink('hostaddr=189.51.156.9 dbname=ins user=postgres password=i745@postgres port=5432'::text, 'select e.idcliente,
case when e.tipoequipamento = 1 then ''CABLE MODEM''
when e.tipoequipamento = 2 then ''CPE DHCP''
when e.tipoequipamento = 3 then ''CPE RADIUS''
when e.tipoequipamento = 4 then ''DECODER''
when e.tipoequipamento = 5 then ''CPE HOT SPOT''
when e.tipoequipamento = 6 then ''EMTA''
when e.tipoequipamento = 7 then ''CPE RADIUS POR DHCP''
when e.tipoequipamento = 8 then ''TELEFONIA''
when e.tipoequipamento = 9 then ''ONU''
end as tipoequipamento,
case when p.nomeplano is not null and p.nomeplano <> '''' then p.nomeplano else ''LINK LÓGICO: ''||pe.linklogico||''- PLANO: ''||pl.nomeplano end as nomeplano
from idhcp.equipamentos e
left join idhcp.planos p on p.id = e.idplano
left join idhcp.planosequipamento pe on pe.idequipamento = e.id
left join idhcp.planos pl on pl.id = pe.idplano
where e.idmotivosderetirada is null and e.tipoequipamento not in (9)
order by e.idcliente'::text) t(idcliente bigint, tipoequipamento character varying, nomeplano character varying)
)
SELECT DISTINCT
e.razaosocial AS "Base",
cl.cidade,
cid.nomedacidade AS "Cidade",
cl.codigocliente AS "Codigo_Assinante",
cl.nome AS "Nome_Assinante",
ct.contrato AS "Contratos",
CASE
WHEN ct.situacao = 1 THEN 'Aguard. Conexão'::text
WHEN ct.situacao = 2 THEN 'ConectadoAtivo'::text
WHEN ct.situacao = 3 THEN 'Pausado'::text
WHEN ct.situacao = 4 THEN 'Inadimplente'::text
WHEN ct.situacao = 5 THEN 'Cancelado'::text
WHEN ct.situacao = 6 THEN 'EndereçoNaoCabeado'::text
WHEN ct.situacao = 7 THEN 'ConectadoInadimplente'::text
ELSE NULL::text
END AS "Situacao",
pr.codigodaprogramacao AS "Cod_Programação",
pr.nomedaprogramacao AS "Programacoes",
pr.nomeabreviado AS "Descricao_Abreviada_Programacao",
pc.descricao AS "Pacote Cas",
pp.nome AS "Programacao Play",
tec.descricaotecnologia AS "Tipo_Tecnologia",
CASE
WHEN cl.cidade IN (803221, 831511, 849991, 852271, 861181) THEN a.tipoequipamento
WHEN cl.cidade IN (805511, 870411) THEN b.tipoequipamento
WHEN cl.cidade IN (817951, 865841, 883311) THEN c.tipoequipamento
WHEN cl.cidade = 891681 THEN d.tipoequipamento
ELSE f.tipoequipamento
END AS "Tipo_Equipamento"
FROM clientes cl
JOIN cidade cid ON cid.codigodacidade = cl.cidade
JOIN contratos ct ON ct.cidade = cl.cidade AND ct.codigodocliente = cl.codigocliente
JOIN empresas e ON e.codcidade = ct.cidade AND e.codempresa = ct.codempresa
JOIN cont_prog cp ON cp.cidade = ct.cidade AND cp.codempresa = ct.codempresa AND cp.contrato = ct.contrato
JOIN programacao pr ON pr.codcidade = cp.cidade AND pr.codigodaprogramacao = cp.protabelaprecos
LEFT JOIN idpacotescas ip ON ip.codigocidade = pr.codcidade AND ip.codigoprogramacao = pr.codigodaprogramacao
LEFT JOIN pacotescas pc ON ip.codigopacotescas = pc.codigo
LEFT JOIN programacaopacotesplay ppp ON ppp.codigocidadeprogramacao = PR.codcidade AND ppp.codigoprogramacao = PR.codigodaprogramacao
LEFT OUTER JOIN programacaoplay pp ON ppp.codprogramacaopacoteplay = pp.codigoprogramacaoplay
LEFT JOIN tipotecnologiapacote tec ON tec.codtipotecnologia = pr.codtipotecnologia
LEFT JOIN a ON a.idcliente = ct.id
LEFT JOIN b ON b.idcliente = ct.id
LEFT JOIN c ON c.idcliente = ct.id
LEFT JOIN d ON d.idcliente = ct.id
LEFT JOIN f ON f.idcliente = ct.id
WHERE ct.situacao <> 5
ORDER BY cid.nomedacidade, cl.nome

--------------------------------------------------------------------------------------------------------------------------------------------

A solução foi encontrada e demanda solucionada! 
Estou encerrando este ticket. 
Lembre-se, estamos sempre a disposição!!!:blush:

Assim que finalizarmos este ticket, terá uma pesquisa com duas perguntas sobre o meu atendimento. Ficarei muito grato(a) se pudesse avaliar :grin:
 
Até a próxima.

--------------------------------------------------------------------------------------------------------------------------------------------

-- VIEW RELATÓRIO CLIENTES ATIVOS SEM INS
CREATE OR REPLACE VIEW regrasoperacao.vis_clientes_ativos_sem_ins(
nome,
contrato,
nomedacidade,
situacao)
AS
  SELECT cli.nome,
         ct.contrato,
         cid.nomedacidade,
         case
         	when ct.situacao = 1 then 'Aguardando'
         	when ct.situacao = 2 then 'Conectado'
         	when ct.situacao = 3 then 'Pausado'
         	when ct.situacao = 4 then 'Inadimplente'
         	when ct.situacao = 5 then 'Cancelado'
         	when ct.situacao = 6 then 'Endereço não Cabeado'
         	when ct.situacao = 7 then 'Conectado/Inadimplente'
         end as "Situação"
from contratos ct
     join clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
     left join cidade cid on ct.cidade = cid . codigodacidade
     left join (SELECT t.idcliente,
t.tipoequipamento,
t.nomeplano
FROM dblink('hostaddr=150.230.79.177 dbname=ins user=postgres password=i745@postgres port=5432'::text, 'select e.idcliente,
case when e.tipoequipamento = 1 then ''CABLE MODEM''
when e.tipoequipamento = 2 then ''CPE DHCP''
when e.tipoequipamento = 3 then ''CPE RADIUS''
when e.tipoequipamento = 4 then ''DECODER''
when e.tipoequipamento = 5 then ''CPE HOT SPOT''
when e.tipoequipamento = 6 then ''EMTA''
when e.tipoequipamento = 7 then ''CPE RADIUS POR DHCP''
when e.tipoequipamento = 8 then ''TELEFONIA''
when e.tipoequipamento = 9 then ''ONU''
end as tipoequipamento,
case when p.nomeplano is not null and p.nomeplano <> '''' then p.nomeplano else ''LINK LÓGICO: ''||pe.linklogico||''- PLANO: ''||pl.nomeplano end as nomeplano
from idhcp.equipamentos e
left join idhcp.planos p on p.id = e.idplano
left join idhcp.planosequipamento pe on pe.idequipamento = e.id
left join idhcp.planos pl on pl.id = pe.idplano
where e.idmotivosderetirada is null and e.tipoequipamento not in (9)
order by e.idcliente'::text) t(idcliente bigint, tipoequipamento character varying, nomeplano character varying)
) as t on t.idcliente = ct.id
where t.idcliente is NULL and ct.situacao NOT IN (5, 6);

--------------------------------------------------------------------------------------------------------------------------------------------

--RELATÓRIO CLIENTES QUE NÃO GERAM BOLETOS CONECTCOR
with
  notafiscal as (
      select t.numfatura, t.tiponf, sum(t.totalnota) as totalnota, max(t.d_dataemissao) as d_dataemissao, max(t.d_datacancelamento) as d_datacancelamento 
      from (
        select distinct i.numfatura, nf.tiponf, nf.totalnota, nf.d_dataemissao, nf.d_datacancelamento
        from itensnf i
        join nfviaunica nf on nf.id = i.idnfconvenio
        join cidade cid on cid.codigodacidade=nf.codcidade
        join docreceber dr on dr.fatura=i.numfatura
        join contratos ct on ct.cidade=nf.codcidade and ct.codempresa=nf.codempresa and ct.contrato=nf.numcontrato
     ) as t
      group by t.numfatura, t.tiponf  
  ) ,
  itens as (
    select t.id, t.tipo, sum(t.valoros) as valor
    from (
      select dr.id, 
      case when p.id is not null and p.tipoprogramacao = 0 then 'TV'
           when p.id is not null and p.tipoprogramacao = 1 then 'Internet'
           when p.id is not null and p.tipoprogramacao = 8 then 'Telefonia'
           else 'Outros'
      end as tipo, m.valoros
      from docreceber dr
      join movimfinanceiro m on m.numfatura=dr.fatura
      left join programacao p on p.codcidade=m.cidade and p.codigodaprogramacao=m.numerodaprogramacao
      left join lanceservicos l on l.codigodoserv_lanc = m.lanc_servico
    ) as t
    group by t.id, t.tipo
)
SELECT DISTINCT 
       cid.nomedacidade,
       cli.codigocliente,
       cli.nome,
       nfd.numfatura as numero_nota_debito, 
       nfd.totalnota as valor_nota_debito, 
       nfd.d_dataemissao as data_nota_debito,
       nf.numfatura as numero_nota_telecom, 
       nf.totalnota as valor_nota_telecom, 
       nf.d_dataemissao as data_nota_telecom,
       i.valor as valor_internet, 
       ite.valor as valor_telefonia, 
       it.valor as valor_tv, 
       ito.valor as valor_outros,
       /*dr.numerodocumento,*/
       dr.valordocumento,
       dr.d_datavencimento,
       dr.valordesconto,
       dr.valormulta,
       dr.valorjuros,
       dr.valorpago,
       dr.d_dataliquidacao,
       CASE
         WHEN dr.tipopagamento = ANY (ARRAY [ 1, 3 ]) THEN 'Dinheiro'::text
         WHEN dr.tipopagamento = 2 THEN 'Cheque'::text
         WHEN dr.tipopagamento = 4 THEN 'Cartão de Débito'::text
         WHEN dr.tipopagamento = 5 THEN 'Cartão de Crédito'::text
         ELSE ''::text
       END as Forma_Pagamento/*,
       l.descricao
       --cli.cpf_cnpj*/
FROM docreceber dr
join movimfinanceiro m on m.numfatura=dr.fatura
JOIN contratos ct ON ct.cidade = m.cidade AND ct.codempresa = m.codempresa AND ct.contrato = m.contrato
JOIN clientes cli ON cli.cidade = dr.codigodacidade AND cli.codigocliente = dr.cliente
JOIN localcobranca l ON l.codigo = dr.localcobranca
JOIN cidade cid ON cid . codigodacidade = dr.codigodacidade
left join itens i on i.id=dr.id and i.tipo = 'Internet'
left join itens it on it.id=dr.id and it.tipo = 'TV'
left join itens ite on ite.id=dr.id and ite.tipo = 'Telefonia'
left join itens ito on ito.id=dr.id and ito.tipo = 'Outros'
LEFT join notafiscal nf on nf.numfatura = dr.fatura and nf.tiponf = 1
LEFT join notafiscal nfd on nfd.numfatura = dr.fatura and nfd.tiponf = 3
where ct.situacao <> 5 and dr.d_datafaturamento BETWEEN '2022-05-01' and '2022-05-31' 
and (dr.enviadoparabanco = 0 OR DR.enviadoparabanco IS null) 
and dr.formadepagamento = 1 and ct.faturaimpressa = 2

--------------------------------------------------------------------------------------------------------------------------------------------

-- RELATÓRIO BOLETOS ABERTO MES MATCHA
select c.nomedacidade,
       dr.cliente,
       cl.nome,
       array_agg(split_part(m.observacao::text, '/'::text, 1)) AS servico,
       dr.nossonumero,
       dr.valordocumento,
       dr.valordesconto,
       dr.valorpago,
       CASE
       when dr.valorpago <> 0 then 'Sim'
       else 'Não' end as Recebido,
       dr.valorjuros,
       dr.valormulta,
       dr.d_datavencimento,
       dr.d_datapagamento,
       l.descricao as Local_Cobranca
              
from docreceber dr
     join movimfinanceiro m on m.numfatura = dr.fatura
     join cidade c on c.codigodacidade = dr.codigodacidade
     join clientes cl on cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente
     left join localcobranca l on l.codigo = dr.localcobranca
where dr.d_datavencimento BETWEEN '2021-06-01' and '2021-06-30'
      group by c.nomedacidade,
               dr.cliente,
               cl.nome,
               dr.nossonumero,
               dr.valordocumento,
               dr.valordesconto,
               dr.valorpago,
               Recebido,
               dr.valorjuros,
               dr.valormulta,
               dr.d_datavencimento,
               dr.d_datapagamento,
               l.descricao
      order by c.nomedacidade

--------------------------------------------------------------------------------------------------------------------------------------------

-- Relatório DICI

-- DICI INTERNET 

select
empresacoleta as "CNPJ", anocoleta as "ANO", mescoleta as "MES",
municipioibge as "COD_IBGE", tipocliente as "TIPO_CLIENTE", tipoatendimento as "TIPO_ATENDIMENTO",
tipomeioacesso as "TIPO_MEIO", tipoproduto as "TIPO_PRODUTO", tecnologia as "TIPO_TECNOLOGIA",
velocidadecontratada as "VELOCIDADE", sum(quantidade) as "QT_ACESSOS"
from public.funcao_dice_anatel_v3(
null, -- Código da Regional, se for todas deixar a palavra null
'2021-04-01'::date, -- Mês de Extração, colocar no formato YYYY-MM-DD, pode ser usado qualquer dia do mês, mas de preferência 01
1::smallint -- Tipo de Extração [1]Internet [2] TV
)group by 1, 2, 3, 4, 5, 6, 7,8,9,10 -- Internet


-- DICI TV

select
empresacoleta as "CNPJ", anocoleta as "ANO", mescoleta as "MES",
municipioibge as "COD_IBGE", tipocliente as "TIPO_CLIENTE",
tipomeioacesso as "TIPO_MEIO",tecnologia as "TIPO_TECNOLOGIA",
sum(quantidade) as "QT_ACESSOS"
from public.funcao_dice_anatel_v3(
null, -- Código da Regional, se for todas deixar a palavra null
'2021-01-01'::date, -- Mês de Extração, colocar no formato YYYY-MM-DD, pode ser usado qualquer dia do mês, mas de preferência 01
2::smallint -- Tipo de Extração [1]Internet [2] TV
)group by 1, 2, 3, 4, 5, 6, 7,8,9,10 -- TV

--Lembrando que a data colocada no select é referente ao mês da extração e o campo empresa deve ser editado no Excel posteriormente com o CNPJ da empresa.

--------------------------------------------------------------------------------------------------------------------------------------------

-- RELATÓRIOS CLIENTES CANCELADOS INTERNET MAIS
with
w as (
		select con.id
        from  ordemservico  os
        join contratos con on con.contrato=os.codigocontrato and con.cidade=os.cidade and con.codempresa=os.codempresa
        where os.codservsolicitado in (81,1251,411) 
        and os.observacoes ilike '%OS GERADA PELA R%' and os.situacao = 3
),
x as (
select x.id , o.id as idos, mc.descmotivo
from (
  select DISTINCT con.id, max(os.id) as idos
  from  ordemservico os
  join lanceservicos ls on ls.codigodoserv_lanc = os.codservsolicitado
  join contratos con on con.contrato=os.codigocontrato and con.cidade=os.cidade and con.codempresa=os.codempresa
  where ls.situacaocontrato =5 
  group by con.id
  ) as x
  join ordemservico o on o.id=x.idos
  left join motivocancelamento mc on mc.codmotivo = o.motivocancelamento 
),
y as (  
 select distinct  cli.id , case
            when dr.d_dataspc is not null and dr.d_dataexclusaospc is null then
              'SIM'
            else 'NÃO'
          end as spc
 from docreceber dr 
 join clientes cli on cli.codigocliente = dr.cliente and cli.cidade = dr.codigodacidade
 ),
 z as (
select distinct ct.id
from contratos ct 
join ordemservico os on os.codigocontrato = ct.contrato and os.cidade = ct.cidade and os.codempresa = ct.codempresa
where os.codservsolicitado = 1311
 ) 
select DISTINCT cli.codigocliente, cli.nome, ct.contrato, ct.cidade,
(select t.fonecompleto
from telefones t
where t.cidade=ct.cidade and t.codigocliente=ct.codigodocliente
limit 1) as telefone_1 ,
(select t.fonecompleto
from telefones t
where t.cidade=ct.cidade and t.codigocliente=ct.codigodocliente
limit 1) as telefone_2,
case when w.id is null then 'CANCELADO ADIMPLENTE' else 'CANCELADO INADIMPLENTE' end as situacao, x.descmotivo,
case when y.id is not null then 'SIM' else 'NÃO' end as spc,
case when z.id is null then 'NÃO' else 'SIM' end as os_retirada
from contratos ct
join clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
left join w on w.id = ct.id
join x on x.id = ct.id
left join y on y.id = cli.id and y.spc = 'SIM'
left join z on z.id = ct.id
join vis_situacaocontrato_descricao v on v.situacao=ct.situacao

where v.situacao in (5)

--------------------------------------------------------------------------------------------------------------------------------------------

-- CLIENTES ADIMPLENTES
with 
x as(
select DISTINCT cl.nome, cl.codigocliente, ct.contrato, 'inadimplente' as Situacao, dr.d_datapagamento, cl.cidade  from docreceber dr
join fatura f on f.numerofatura = dr.fatura
join contratos ct on ct.cidade = f.codigodacidade and ct.contrato = f.numerodocontrato
join clientes cl on cl.codigocliente = ct.codigodocliente and cl.cidade = ct.cidade
where ct.situacao <> 5 and dr.situacao = 0 and dr.d_datapagamento is null and dr.d_datavencimento < CURRENT_DATE
)
select DISTINCT 
       cl.nome,
       cl.codigocliente,
       --ct.contrato,
case    
       when x.nome is NULL then 'Adimplente' else 'Inadimplente'
end as Situacao,
       cl.cidade
from docreceber dr
     join fatura f on f.numerofatura = dr.fatura
     join contratos ct on ct.cidade = f.codigodacidade and ct.contrato = f.numerodocontrato and ct.codempresa = f.codempresa
     join clientes cl on cl.codigocliente = ct.codigodocliente and cl.cidade = ct.cidade
     left join x on x.codigocliente = cl.codigocliente and x.cidade = cl.cidade
where ct.situacao <> 5 and
      dr.d_datavencimento < CURRENT_DATE

--------------------------------------------------------------------------------------------------------------------------------------------

-- INSERE DESCONTO BASE COM PROGRAMAÇÃO
select * from temporarias.inclui_desconto_horas_tiraprog(
	886841,
    130,
    '2022-06-30',
    '71',
    'INDISPONIBILIDADE SERVIÇOS DE INTERNET - 30/06/2022',
    1
)

--------------------------------------------------------------------------------------------------------------------------------------------

-- INSERE DESCONTO BASE SEM PROGRAMAÇÃO
select * from temporarias.inclui_desconto_horas(
	895401,
    130,
    '2022-06-30',
    '',
    'INDISPONIBILIDADE SERVIÇOS DE INTERNET - 30/06/2022',
    2
)

--------------------------------------------------------------------------------------------------------------------------------------------

--RELATÓRIO ASSINANTE X PROGRAMAÇÃO E TECNOLOGIA_R&R CÁSSIA BRASILNET
with
ativ as (
select w.cidade,
       w.codempresa,
       w.contrato,
       w.numos,
       sum(w.valorpacote_ativacao) as valorpacote_ativacao,
       sum(w.valor_desconto_ativacao) as valor_desconto_ativacao,
       w.pacote_ativacao
from (
       select t.cidade,
              t.codempresa,
              t.contrato,
              t.numos,
              t.valorpacote_ativacao,
              t.valor_desconto_ativacao,
              array_to_string(ARRAY
              (
                select distinct p.nomedaprogramacao
                from variacaodepacotes v
                     join programacao p on p.codcidade = v.cidade and p.codigodaprogramacao = v.pacote
                where v.cidade = t.cidade and
                      v.codempresa = t.codempresa and
                      v.numos = t.numos and
                      v.operacao = 1
              ), ' - ', '') as pacote_ativacao
       from (
              select v.cidade,
                     v.codempresa,
                     v.contrato,
                     v.numos,
                     p.codigodaprogramacao,
                     case
                       when v.operacao = 1 then p.nomedaprogramacao
                       else ''
                     end as pacote_ativacao,
                     case
                       when v.operacao = 1 then v.valorpacote
                       else 0
                     end as valorpacote_ativacao,
                     case
                       when v.operacao = 1 then public.func_calculavaloraditivos_v2(v.cidade, v.codempresa, v.contrato, p.tipoponto::integer,
                         p.tipoprogramacao::integer, v.valorpacote -(v.valorpacote * tc.desconto / 100), '2022-01-01'::date, '2022-06-23'::date,
                         v.pacote::integer)
                       else 0
                     end as valor_desconto_ativacao
              from variacaodepacotes v
                   join programacao p on p.codcidade = v.cidade and p.codigodaprogramacao = v.pacote
                   join contratos ct on ct.cidade = v.cidade and ct.codempresa = v.codempresa and ct.contrato = v.contrato
                   join tiposcontrato tc on tc.codigo = ct.tipodocontrato
              where v.operacao = 1 and
                    v.d_data between '2022-01-01'::date and
                    '2022-06-23'::date
            ) as t
     ) as w
group by w.cidade,
         w.codempresa,
         w.contrato,
         w.numos,
         w.pacote_ativacao),



dest as (
select w.cidade,
       w.codempresa,
       w.contrato,
       w.numos,
       sum(w.valorpacote_desativacao) as valorpacote_desativacao,
       sum(w.valor_desconto_desativacao) as valor_desconto_desativacao,
       w.pacote_desativacao
from (
       select t.cidade,
              t.codempresa,
              t.contrato,
              t.numos,
              t.valorpacote_desativacao,
              t.valor_desconto_desativacao,
              array_to_string(ARRAY
              (
                select distinct p.nomedaprogramacao
                from variacaodepacotes v
                     join programacao p on p.codcidade = v.cidade and p.codigodaprogramacao = v.pacote
                where v.cidade = t.cidade and
                      v.codempresa = t.codempresa and
                      v.numos = t.numos and
                      v.operacao = 2
              ), ' - ', '') as pacote_desativacao
       from (
              select v.cidade,
                     v.codempresa,
                     v.contrato,
                     v.numos,
                     case
                       when v.operacao = 2 then p.nomedaprogramacao
                       else ''
                     end as pacote_desativacao,
                     case
                       when v.operacao = 2 then v.valorpacote
                       else 0
                     end as valorpacote_desativacao,
                     case
                       when v.operacao = 2 then public.func_calculavaloraditivos_v2(v.cidade, v.codempresa, v.contrato, p.tipoponto::integer,
                         p.tipoprogramacao::integer, v.valorpacote -(v.valorpacote * tc.desconto / 100), '2022-01-01'::date, '2022-06-23'::date,
                         v.pacote::integer)
                       else 0
                     end as valor_desconto_desativacao
              from variacaodepacotes v
                   join programacao p on p.codcidade = v.cidade and p.codigodaprogramacao = v.pacote
                   join contratos ct on ct.cidade = v.cidade and ct.codempresa = v.codempresa and ct.contrato = v.contrato
                   join tiposcontrato tc on tc.codigo = ct.tipodocontrato
              where v.operacao = 2 and
                    v.d_data between '2022-01-01'::date and
                    '2022-06-23'::date
            ) as t
     ) as w
group by w.cidade,
         w.codempresa,
         w.contrato,
         w.numos,
         w.pacote_desativacao),
ordserv as (
select distinct os.cidade,
       os.codempresa,
       os.numos
from public.ordemservico os
     join public.lanceservicos l on l.codigodoserv_lanc = os.codservsolicitado
where os.d_dataexecucao between '2022-01-01'::date and
      '2022-06-23'::date /*and l.baixapontosmarcados = 4*/)
select distinct os.d_dataatendimento,
       os.numos,
       l.descricaodoserv_lanc,
       CASE
         WHEN length(translate(cli.cpf_cnpj::text, ' .,:-//\_+='::text, ''::text)) = 14 THEN 'Pessoa Juridica'::text
         ELSE 'Pessoa Fisica'::text
       END AS tipo_pessoa,
       cli.codigocliente,
       cli.nome,
       case
         when os.situacao = 1 then 'Pendente'
         when os.situacao = 2 then 'Atendimento'
         when os.situacao = 3 then 'Executada'
       End as situacao,
       tab.nome,
       tab.estado,
       '' as "grupo_cadastro",
       ct.contrato,
       '' as "codigo_plano",
       a.pacote_ativacao,
       a.valor_desconto_ativacao as "valor_pacote_ativacao",
       v.descricaosituacao,
       os.atendente,
       '' as "setor",
       '' as "status",
       '' as "ocorrencia",       
       d.pacote_desativacao,
       d.valor_desconto_desativacao as "valor_pacote_desativacao",
       (a.valorpacote_ativacao - d.valorpacote_desativacao) as "valor_diferencafinal_pacotes",
       pg.nomeabreviado,
       cp.valorpacote,
       '' as "comissao",
       '' as "upgrade_plano"
from ordserv oo
     join ordemservico os on os.cidade = oo.cidade and os.codempresa = oo.codempresa and os.numos = oo.numos
     join contratos ct on ct.cidade = os.cidade and ct.codempresa = os.codempresa and ct.contrato = os.codigocontrato
     join cidade cid on cid . codigodacidade = os.cidade
     LEFT JOIN cont_prog cp ON cp.cidade = ct.cidade AND cp.contrato = ct.contrato
     LEFT JOIN programacao pg ON pg.codigodaprogramacao = cp.protabelaprecos AND pg.codcidade = cp.cidade
     join regional r on r.codigo = cid . codigo_regional
     JOIN tablocal tab ON tab.codigo = ct.cidade
     join clientes cli on cli.cidade = os.cidade and cli.codigocliente = os.codigoassinante
     join lanceservicos l on l.codigodoserv_lanc = os.codservsolicitado
     left join hwusers h on lower(h.login) = lower(os.atendente)
     left join hwgroups hg on hg.id = h.groupid
     join vis_situacaocontrato_descricao v on v.situacao = ct.situacao
     left join ativ a on a.cidade = oo.cidade and a.codempresa = oo.codempresa and a.numos = oo.numos
     left join dest d on d.cidade = a.cidade and d.codempresa = a.codempresa and d.numos = a.numos
where os.d_dataexecucao between '2022-01-01' and '2022-06-23' /*and l.baixapontosmarcados = 4*/ and
 l.codigodoserv_lanc IN (111181, 111191, 111201, 111161, 111151, 111171)

--------------------------------------------------------------------------------------------------------------------------------------------

-- ALTERAÇÃO UPDATE BAIRROS TUPÃ
CREATE TABLE temporarias.de_para_bairro (
  id SERIAL,
  bairroantigo TEXT,
  bairronovo TEXT,
  bairroreduzido TEXT
) 
WITH (oids = false);

ALTER TABLE temporarias.de_para_bairro
  OWNER TO postgres;

select b.bairroantigo, b.bairronovo, b.bairroreduzido, length(b.bairroreduzido)
from temporarias.de_para_bairro b
where length(b.bairroreduzido) > 20  



update contratos set bairroconexao = x.bairroreduzido 
from (
  select ct.id, ct.bairroconexao, b.bairroantigo, b.bairroreduzido
  from contratos ct
  join temporarias.de_para_bairro b on b.bairroantigo=ct.bairroconexao
  where ct.bairroconexao <> b.bairroreduzido
) as x
where x.id=contratos.id


update ceps set bairroinicial = x.bairroreduzido
from (
  select t.*, c.cep, c.bairroinicial, c.id as idcep
  from temporarias.de_para_bairro t
  left join ceps c on upper(c.bairroinicial) = upper(t.bairroantigo) and c.codigodacidade = 891681
  left join enderecos e on e.codigodacidade=c.codigodacidade and e.codigodologradouro=c.logradouro
  where upper(c.bairroinicial) <> upper(t.bairroreduzido)
) as x
where x.idcep=public.ceps.id;


update contratos set bairroconexao = x.bairroinicial
from (
  select c.id, c.bairroconexao, ce.bairroinicial
  from contratos c
  join ceps ce on ce.codigodacidade=c.cidade and ce.logradouro=c.enderecoconexao and ce.cep=c.cepconexao
  where c.bairroconexao <> ce.bairroinicial
  and c.cidade = 891681 and length(ce.bairroinicial) <= 20
) as x
where x.id=public.contratos.id;

update contratos set bairrocobranca = x.bairroinicial
from (
  select c.id, c.bairrocobranca, ce.bairroinicial 
  from contratos c
  join ceps ce on ce.codigodacidade=c.cidadecobranca and ce.logradouro=c.enderecodecobranca and ce.cep=c.cepcobranca
  where c.bairrocobranca <> ce.bairroinicial 
  and c.cidade = 891681 and length(ce.bairroinicial) <= 20
) as x
where x.id=public.contratos.id;

update contratos set bairronota = x.bairroinicial
from (
  select c.id, c.bairronota, ce.bairroinicial 
  from contratos c
  join ceps ce on ce.codigodacidade=c.cidadenota and ce.logradouro=c.endereconota and ce.cep=c.cepnota
  where c.bairronota <> ce.bairroinicial
  and c.cidade = 891681 and length(ce.bairroinicial) <= 20
) as x
where x.id=public.contratos.id;


update clientes set bairroresidencial = x.bairroinicial
from (
  select c.id, c.bairroresidencial, ce.bairroinicial 
  from clientes c
  join ceps ce on ce.codigodacidade=c.cidade and ce.logradouro=c.enderecoresidencial and ce.cep=c.cepresidencial
  where c.bairroresidencial <> ce.bairroinicial
  and c.cidade = 891681 and length(ce.bairroinicial) <= 20
) as x
where x.id=public.clientes.id;

update clientes set bairrocobranca = x.bairroinicial
from (
  select c.id, c.bairrocobranca, ce.bairroinicial 
  from clientes c
  join ceps ce on ce.codigodacidade=c.cidadecobranca and ce.logradouro=c.enderecodecobranca and ce.cep=c.cepcobranca
  where c.bairrocobranca <> ce.bairroinicial
  and c.cidade = 891681 and length(ce.bairroinicial) <= 20
) as x
where x.id=public.clientes.id;

--------------------------------------------------------------------------------------------------------------------------------------------

-- DADOS CADASTRAIS HILTON
select translate(cli.cpf_cnpj::text, '.-/'::text, ''::text) as cnpj_cpf,
       cli.nome as cliente_nome,
       CASE WHEN length(translate(cli.cpf_cnpj::text, '.-/'::text, ''::text)) > 11 THEN cli.nome
        ELSE '' 
       END AS razao_social,
       cli.inscrest_rg as doc_ie_rg,
       '' as DOC_INSCRICAO_MUNICIPAL,
       '' as DOC_OUTRO,
       cli.d_datanascimento,
       CASE WHEN cli.sexo = 1 THEN 'MASCULINO'
       	ELSE 'FEMININO' 
       END AS IND_SEXO,
       CASE	WHEN length(translate(cli.cpf_cnpj::text, '.-/'::text, ''::text)) > 11 THEN 'PJ'::text
       	ELSE 'PF'::text
       END AS IND_PF_PJ,
       ed.tipodologradouro,
       ed.nomelogradouro,
       ct.numerocobranca,
       cli.bairrocobranca,
       cid.nomedacidade,
       tab.estado,
       cli.cepcobranca,
       cli.email,
       split_part(func_retornatelefones(ct.cidade, ct.codigodocliente), '/'::text, 1) as telefone_1,
       split_part(func_retornatelefones(ct.cidade, ct.codigodocliente), '/'::text, 2) as telefone_2,
       split_part(func_retornatelefones(ct.cidade, ct.codigodocliente), '/'::text, 3) as telefone_3,
       split_part(func_retornatelefones(ct.cidade, ct.codigodocliente), '/'::text, 4) as telefone_4,
       cli.nomemae,
       cli.nomepai
from clientes cli
JOIN contratos ct ON ct.codigodocliente = cli.codigocliente AND ct.cidade = cli.cidade
JOIN cidade cid on cid.codigodacidade = cli.cidade
JOIN tablocal tab ON tab.codigo = ct.cidade
JOIN enderecos ed ON ed.codigodacidade = cli.cidade and ed.codigodologradouro = cli.enderecoresidencial

--------------------------------------------------------------------------------------------------------------------------------------------

-- DADOS DE CONTRATO HILTON
select translate(cli.cpf_cnpj::text, '.-/'::text, ''::text) as cnpj_cpf,
       ct.contrato,
       cli.email,
       ct.d_datadavenda,
       ct.d_datadainstalacao as ativacao,
       ct.d_datadainstalacao as cobranca,
       '' as periodo_vigencia,
       ct.valordocontrato as valor_instalacao,
       sum(cp.valorpacote) as valor_contrato,
       CASE WHEN ct.situacao = 1 THEN 'Aguardando Conexão'
            WHEN ct.situacao = 2 THEN 'Conectado/Ativo'
            WHEN ct.situacao = 3 THEN 'Pausado'
            WHEN ct.situacao = 4 THEN 'Inadimplente'
            WHEN ct.situacao = 5 THEN 'Cancelado'
            WHEN ct.situacao = 6 THEN 'Sem Cabeamento'
            WHEN ct.situacao = 7 THEN 'Conectado/Inadimplente'
       END AS situacao_contrato,
       '' as designacao_contrato,
       ct.dtvencto,
       max(os.d_dataexecucao) as data_bloqueio,
       CASE WHEN ct.formapagamento = 1 THEN 'Boleto Bancário'
       		WHEN ct.formapagamento = 2 THEN 'Depósito Bancário'
            WHEN ct.formapagamento = 3 THEN 'Débito em Conta'
       ELSE 'Cartão de Crédito'
       END AS forma_pagamento,
       bc.nome,
       split_part(cc.agencia,'-', 1) as agencia_conta,
       split_part(cc.agencia,'-', 2) as agencia_digito,
       split_part(cc.conta,'-', 1) as conta,
       split_part(cc.conta,'-', 2) as conta_digito,
       cc.convenio,
       '' as codigo_vindi
from contratos ct
JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
LEFT JOIN cont_prog cp ON cp.cidade = ct.cidade AND cp.codempresa = ct.codempresa AND cp.contrato = ct.contrato
JOIN ordemservico os on os.codigocontrato = ct.contrato and os.cidade = ct.cidade and os.codempresa = ct.codempresa
JOIN contascreditocidade cc on cc.codigocidade=ct.cidade and cc.codigoconta=ct.codcontacredito
JOIN bancos bc ON bc.numero = cc.banco
JOIN lanceservicos l on l.codigodoserv_lanc = os.codservsolicitado
GROUP BY cnpj_cpf,
         ct.contrato,
         cli.email,
         ct.d_datadavenda,
         ativacao,
         cobranca,
         periodo_vigencia,
         valor_instalacao,
         ct.d_dataultimaatualizacao,
         situacao_contrato,
         designacao_contrato,
         ct.dtvencto,
         forma_pagamento,
         bc.nome,
         agencia_conta,
         agencia_digito,
         conta,
         conta_digito,
         cc.convenio,
         codigo_vindi

--------------------------------------------------------------------------------------------------------------------------------------------

-- DADOS FINANCEIRO HILTON
SELECT DISTINCT
	   dr.id,
       ct.contrato,
       dr.d_dataemissao,
       dr.d_datavencimento,
       dr.valordocumento,
       CASE
        WHEN dr.d_datacancelamento is not null then 'Cancelado'
        WHEN dr.d_datapagamento is null then 'aberto'
       ELSE 'Pago' end as Status,
       dr.numerodocumento,
       CASE
        WHEN dr.formadepagamento = 1 THEN 'Boleto Bancário'
        WHEN dr.formadepagamento = 2 THEN 'Depósito Bancário'
        WHEN dr.formadepagamento = 3 THEN 'Débito Automático'
        WHEN dr.formadepagamento = 4 THEN 'Cartão de Crédito' 
       END AS "Forma_Pagamento",    
       dr.nossonumero
FROM docreceber dr
     JOIN cidade c ON c.codigodacidade = dr.codigodacidade
     JOIN public.fatura f ON f.numerofatura = dr.fatura
     JOIN public.contratos ct ON ct.cidade = f.codigodacidade and ct.contrato = f.numerodocontrato and ct.codempresa = f.codempresa
     JOIN public.clientes cl ON cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente
     where dr.id > 1252944
     order by dr.id

--------------------------------------------------------------------------------------------------------------------------------------------

--ITENS CONTRATO HILTON

--MEGAFIBRA SOROCABA
with 
x as (SELECT t.idcliente,
t.macaddress,
t.serial,
t.username,
t.senha
FROM dblink('hostaddr=131.196.236.22 dbname=ins user=postgres password=i745@postgres port=5432'::text, 
'select e.idcliente,
case when e.tipoequipamento = 9 then ''eq.macaddress'' end as macaddress,
case when e.tipoequipamento = 9 then ''eq.serial'' end as serial,
e.username,
e.senha
from idhcp.equipamentos e
left join idhcp.planos p on p.id = e.idplano
left join idhcp.planosequipamento pe on pe.idequipamento = e.id
left join idhcp.planos pl on pl.id = pe.idplano
where e.idmotivosderetirada is null and e.tipoequipamento not in (9)
order by e.idcliente'::text) t(idcliente bigint, macaddress character varying, serial character varying, username text, senha varchar)
),
y as (SELECT t.idcliente,
t.macaddress,
t.serial,
t.username,
t.senha
FROM dblink('hostaddr=187.111.164.3 dbname=ins user=postgres password=i745@postgres port=5432'::text, 
'select e.idcliente,
case when e.tipoequipamento = 9 then ''eq.macaddress'' end as macaddress,
case when e.tipoequipamento = 9 then ''eq.serial'' end as serial,
e.username,
e.senha
from idhcp.equipamentos e
left join idhcp.planos p on p.id = e.idplano
left join idhcp.planosequipamento pe on pe.idequipamento = e.id
left join idhcp.planos pl on pl.id = pe.idplano
where e.idmotivosderetirada is null and e.tipoequipamento not in (9)
order by e.idcliente'::text) t(idcliente bigint, macaddress character varying, serial character varying, username text, senha varchar)
)
select DISTINCT
ct.contrato,
'1' as qtde,
cp.valorpacote,
sum (cp.valorpacote) as valor_total,
ed.tipodologradouro,
ed.nomelogradouro,
ct.numeroconexao,
ct.enderecoconexao,
ct.bairroconexao,
cid.nomedacidade,
tab.estado,
ct.cepconexao,
CASE
	WHEN cid.codigodacidade = 865681 THEN x.macaddress
    ELSE y.macaddress
END AS macaddressok,
CASE
	WHEN cid.codigodacidade = 865681 THEN x.serial
    ELSE y.serial
END AS serialok,
'' as DESIGNACAO_TECNICA,
'' as WIFI_NOME,
'' as WIFI_SENHA,
CASE
	WHEN cid.codigodacidade = 865681 THEN x.username
    ELSE y.username
END AS usernameok,
CASE
	WHEN cid.codigodacidade = 865681 THEN x.senha
    ELSE y.senha
END AS senhaok,
'' as NUMERO_IP_POP,
'' as NUMERO_SLOT,
'' as NUMERO_PON,
'' as TIPO_ROTEAMENTO,
pr.nomedaprogramacao,
'' as COD_POLICY_DOWN,
'' as NRO_LINHA,
'' as COD_POLICY_UP,
'' as COD_POLICY_NOME,
'' as TIPO_ACESSO,
'' as BLOCO_IP,
'' as CONCENTRADOR_BLOCO_IP,
'' as IP_FIXO
from cont_prog cp
left join cidade cid on cp.cidade = cid.codigodacidade
left join contratos ct on ct.cidade = cp.cidade and ct.contrato = cp.contrato
left join clientes cli on cli.cidade = cp.cidade and ct.codigodocliente = cli.codigocliente
JOIN enderecos ed ON ed.codigodacidade = cli.cidade and ed.codigodologradouro = cli.enderecoresidencial
JOIN tablocal tab ON tab.codigo = ct.cidade
join programacao pr on pr.codcidade = cp.cidade and pr.codigodaprogramacao = cp.protabelaprecos
LEFT JOIN x ON x.idcliente = ct.id
LEFT JOIN y ON y.idcliente = ct.id
GROUP BY ct.contrato,
cp.valorpacote,
ed.tipodologradouro,
ed.nomelogradouro,
ct.numeroconexao,
ct.enderecoconexao,
ct.bairroconexao,
cid.nomedacidade,
tab.estado,
ct.cepconexao,
macaddressok,
serialok,
DESIGNACAO_TECNICA,
WIFI_NOME,
WIFI_SENHA,
usernameok,
senhaok,
NUMERO_IP_POP,
NUMERO_SLOT,
NUMERO_PON,
TIPO_ROTEAMENTO,
pr.nomedaprogramacao,
COD_POLICY_DOWN,
NRO_LINHA,
COD_POLICY_UP,
COD_POLICY_NOME,
TIPO_ACESSO,
BLOCO_IP,
CONCENTRADOR_BLOCO_IP,
IP_FIXO


-- MF PIEDADE

with 
x as (SELECT t.idcliente,
t.macaddress,
t.serial,
t.username,
t.senha
FROM dblink('hostaddr=131.196.236.22 dbname=ins user=postgres password=i745@postgres port=5432'::text, 
'select e.idcliente,
case when e.tipoequipamento = 9 then ''eq.macaddress'' end as macaddress,
case when e.tipoequipamento = 9 then ''eq.serial'' end as serial,
e.username,
e.senha
from idhcp.equipamentos e
left join idhcp.planos p on p.id = e.idplano
left join idhcp.planosequipamento pe on pe.idequipamento = e.id
left join idhcp.planos pl on pl.id = pe.idplano
where e.idmotivosderetirada is null and e.tipoequipamento not in (9)
order by e.idcliente'::text) t(idcliente bigint, macaddress character varying, serial character varying, username text, senha varchar)
)
select DISTINCT
ct.contrato,
'1' as qtde,
cp.valorpacote,
sum (cp.valorpacote) as valor_total,
ed.tipodologradouro,
ed.nomelogradouro,
ct.numeroconexao,
ct.enderecoconexao,
ct.bairroconexao,
cid.nomedacidade,
tab.estado,
ct.cepconexao,
x.macaddress,
x.serial,
'' as DESIGNACAO_TECNICA,
'' as WIFI_NOME,
'' as WIFI_SENHA,
x.username,
x.senha,
'' as NUMERO_IP_POP,
'' as NUMERO_SLOT,
'' as NUMERO_PON,
'' as TIPO_ROTEAMENTO,
pr.nomedaprogramacao,
'' as COD_POLICY_DOWN,
'' as NRO_LINHA,
'' as COD_POLICY_UP,
'' as COD_POLICY_NOME,
'' as TIPO_ACESSO,
'' as BLOCO_IP,
'' as CONCENTRADOR_BLOCO_IP,
'' as IP_FIXO
from cont_prog cp
left join cidade cid on cp.cidade = cid.codigodacidade
left join contratos ct on ct.cidade = cp.cidade and ct.contrato = cp.contrato
left join clientes cli on cli.cidade = cp.cidade and ct.codigodocliente = cli.codigocliente
JOIN enderecos ed ON ed.codigodacidade = cli.cidade and ed.codigodologradouro = cli.enderecoresidencial
JOIN tablocal tab ON tab.codigo = ct.cidade
join programacao pr on pr.codcidade = cp.cidade and pr.codigodaprogramacao = cp.protabelaprecos
LEFT JOIN x ON x.idcliente = ct.id
GROUP BY ct.contrato,
cp.valorpacote,
ed.tipodologradouro,
ed.nomelogradouro,
ct.numeroconexao,
ct.enderecoconexao,
ct.bairroconexao,
cid.nomedacidade,
tab.estado,
ct.cepconexao,
x.macaddress,
x.serial,
DESIGNACAO_TECNICA,
WIFI_NOME,
WIFI_SENHA,
x.username,
x.senha,
NUMERO_IP_POP,
NUMERO_SLOT,
NUMERO_PON,
TIPO_ROTEAMENTO,
pr.nomedaprogramacao,
COD_POLICY_DOWN,
NRO_LINHA,
COD_POLICY_UP,
COD_POLICY_NOME,
TIPO_ACESSO,
BLOCO_IP,
CONCENTRADOR_BLOCO_IP,
IP_FIXO


-- MF SUPERMÍDIA

with 
x as (SELECT t.idcliente,
t.macaddress,
t.serial,
t.username,
t.senha
FROM dblink('hostaddr=187.111.160.20 dbname=ins user=postgres password=i745@postgres port=5432'::text, 
'select e.idcliente,
case when e.tipoequipamento = 9 then ''eq.macaddress'' end as macaddress,
case when e.tipoequipamento = 9 then ''eq.serial'' end as serial,
e.username,
e.senha
from idhcp.equipamentos e
left join idhcp.planos p on p.id = e.idplano
left join idhcp.planosequipamento pe on pe.idequipamento = e.id
left join idhcp.planos pl on pl.id = pe.idplano
where e.idmotivosderetirada is null and e.tipoequipamento not in (9)
order by e.idcliente'::text) t(idcliente bigint, macaddress character varying, serial character varying, username text, senha varchar)
)
select DISTINCT
ct.contrato,
'1' as qtde,
cp.valorpacote,
sum (cp.valorpacote) as valor_total,
ed.tipodologradouro,
ed.nomelogradouro,
ct.numeroconexao,
ct.enderecoconexao,
ct.bairroconexao,
cid.nomedacidade,
tab.estado,
ct.cepconexao,
x.macaddress,
x.serial,
'' as DESIGNACAO_TECNICA,
'' as WIFI_NOME,
'' as WIFI_SENHA,
x.username,
x.senha,
'' as NUMERO_IP_POP,
'' as NUMERO_SLOT,
'' as NUMERO_PON,
'' as TIPO_ROTEAMENTO,
pr.nomedaprogramacao,
'' as COD_POLICY_DOWN,
'' as NRO_LINHA,
'' as COD_POLICY_UP,
'' as COD_POLICY_NOME,
'' as TIPO_ACESSO,
'' as BLOCO_IP,
'' as CONCENTRADOR_BLOCO_IP,
'' as IP_FIXO
from cont_prog cp
left join cidade cid on cp.cidade = cid.codigodacidade
left join contratos ct on ct.cidade = cp.cidade and ct.contrato = cp.contrato
left join clientes cli on cli.cidade = cp.cidade and ct.codigodocliente = cli.codigocliente
JOIN enderecos ed ON ed.codigodacidade = cli.cidade and ed.codigodologradouro = cli.enderecoresidencial
JOIN tablocal tab ON tab.codigo = ct.cidade
join programacao pr on pr.codcidade = cp.cidade and pr.codigodaprogramacao = cp.protabelaprecos
LEFT JOIN x ON x.idcliente = ct.id
GROUP BY ct.contrato,
cp.valorpacote,
ed.tipodologradouro,
ed.nomelogradouro,
ct.numeroconexao,
ct.enderecoconexao,
ct.bairroconexao,
cid.nomedacidade,
tab.estado,
ct.cepconexao,
x.macaddress,
x.serial,
DESIGNACAO_TECNICA,
WIFI_NOME,
WIFI_SENHA,
x.username,
x.senha,
NUMERO_IP_POP,
NUMERO_SLOT,
NUMERO_PON,
TIPO_ROTEAMENTO,
pr.nomedaprogramacao,
COD_POLICY_DOWN,
NRO_LINHA,
COD_POLICY_UP,
COD_POLICY_NOME,
TIPO_ACESSO,
BLOCO_IP,
CONCENTRADOR_BLOCO_IP,
IP_FIXO


-- MF TATUÍ

with 
x as (SELECT t.idcliente,
t.macaddress,
t.serial,
t.username,
t.senha
FROM dblink('hostaddr=187.111.160.7 dbname=ins user=postgres password=i745@postgres port=5432'::text, 
'select e.idcliente,
case when e.tipoequipamento = 9 then ''eq.macaddress'' end as macaddress,
case when e.tipoequipamento = 9 then ''eq.serial'' end as serial,
e.username,
e.senha
from idhcp.equipamentos e
left join idhcp.planos p on p.id = e.idplano
left join idhcp.planosequipamento pe on pe.idequipamento = e.id
left join idhcp.planos pl on pl.id = pe.idplano
where e.idmotivosderetirada is null and e.tipoequipamento not in (9)
order by e.idcliente'::text) t(idcliente bigint, macaddress character varying, serial character varying, username text, senha varchar)
)
select DISTINCT
ct.contrato,
'1' as qtde,
cp.valorpacote,
sum (cp.valorpacote) as valor_total,
ed.tipodologradouro,
ed.nomelogradouro,
ct.numeroconexao,
ct.enderecoconexao,
ct.bairroconexao,
cid.nomedacidade,
tab.estado,
ct.cepconexao,
x.macaddress,
x.serial,
'' as DESIGNACAO_TECNICA,
'' as WIFI_NOME,
'' as WIFI_SENHA,
x.username,
x.senha,
'' as NUMERO_IP_POP,
'' as NUMERO_SLOT,
'' as NUMERO_PON,
'' as TIPO_ROTEAMENTO,
pr.nomedaprogramacao,
'' as COD_POLICY_DOWN,
'' as NRO_LINHA,
'' as COD_POLICY_UP,
'' as COD_POLICY_NOME,
'' as TIPO_ACESSO,
'' as BLOCO_IP,
'' as CONCENTRADOR_BLOCO_IP,
'' as IP_FIXO
from cont_prog cp
left join cidade cid on cp.cidade = cid.codigodacidade
left join contratos ct on ct.cidade = cp.cidade and ct.contrato = cp.contrato
left join clientes cli on cli.cidade = cp.cidade and ct.codigodocliente = cli.codigocliente
JOIN enderecos ed ON ed.codigodacidade = cli.cidade and ed.codigodologradouro = cli.enderecoresidencial
JOIN tablocal tab ON tab.codigo = ct.cidade
join programacao pr on pr.codcidade = cp.cidade and pr.codigodaprogramacao = cp.protabelaprecos
LEFT JOIN x ON x.idcliente = ct.id
GROUP BY ct.contrato,
cp.valorpacote,
ed.tipodologradouro,
ed.nomelogradouro,
ct.numeroconexao,
ct.enderecoconexao,
ct.bairroconexao,
cid.nomedacidade,
tab.estado,
ct.cepconexao,
x.macaddress,
x.serial,
DESIGNACAO_TECNICA,
WIFI_NOME,
WIFI_SENHA,
x.username,
x.senha,
NUMERO_IP_POP,
NUMERO_SLOT,
NUMERO_PON,
TIPO_ROTEAMENTO,
pr.nomedaprogramacao,
COD_POLICY_DOWN,
NRO_LINHA,
COD_POLICY_UP,
COD_POLICY_NOME,
TIPO_ACESSO,
BLOCO_IP,
CONCENTRADOR_BLOCO_IP,
IP_FIXO

--------------------------------------------------------------------------------------------------------------------------------------------

--RELATÓRIO SVA GERAL CONEXÃO
SELECT DISTINCT cid.nomedacidade, 
                pr.codigodaprogramacao,
                pr.nomedaprogramacao,
                CASE
                 WHEN pr.liberadaparavenda = 0 THEN ''NÃO''
                 ELSE ''SIM''
                END AS liberada_venda,
                pr.tier,
                sv.descricao
                
FROM cont_prog cp
JOIN contratos ct ON ct.cidade = cp.cidade AND ct.codempresa = cp.codempresa AND ct.contrato = cp.contrato
JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
JOIN cidade cid ON cid . codigodacidade = ct.cidade
JOIN programacao pr ON pr.codcidade = cp.cidade AND pr.codigodaprogramacao = cp.protabelaprecos
join programacaopacotesplay ppp on ppp.codigoprogramacao = pr.codigodaprogramacao and ppp.codigocidadeprogramacao = pr.codcidade
join programacaoplay pp on pp.codigoprogramacaoplay = ppp.codprogramacaoplay
LEFT JOIN tiposva sv ON sv.codigo = pp.codigotiposva

--------------------------------------------------------------------------------------------------------------------------------------------

-- VALOR BOLETO DIFERENTE DIVERGENTE VALOR NOTA - CONECTCOR
with
notafiscal as (
    select t.numfatura, t.tiponf, sum(t.totalnota) as totalnota
    from (
      select distinct i.numfatura, nf.tiponf, nf.totalnota, nf.d_dataemissao, nf.d_datacancelamento
      from itensnf i
      join nfviaunica nf on nf.id = i.idnfconvenio
      join cidade cid on cid.codigodacidade = nf.codcidade
      join docreceber dr on dr.fatura = i.numfatura
      join contratos ct on ct.cidade = nf.codcidade and ct.codempresa = nf.codempresa and ct.contrato = nf.numcontrato
   ) as t
group by t.numfatura, t.tiponf  
)

SELECT distinct cid.nomedacidade, dr.cliente, cl.nome, dr.valordocumento, nf.totalnota as nota_telecom, nfd.totalnota as nota_debito
from docreceber dr
JOIN cidade cid on cid.codigodacidade = dr.codigodacidade
JOIN fatura ft ON ft.numerofatura = dr.fatura and ft.codigodacidade = dr.codigodacidade
join itensnf i on i.numfatura = ft.numerofatura
join nfviaunica n on n.codcidade = i.codcidade and n.id = i.idnfconvenio
join clientes cl on cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente
LEFT join notafiscal nf on nf.numfatura = dr.fatura and nf.tiponf = 1
LEFT join notafiscal nfd on nfd.numfatura = dr.fatura and nfd.tiponf = 3
where dr.d_datafaturamento BETWEEN '2022-06-01' and '2022-06-30'

--------------------------------------------------------------------------------------------------------------------------------------------

-- RELATÓRIO PERSONALIZADO INADIMPLENTES CONEXÃO
select 
 t.cidade,
 t.codigocliente,
 t.nome,
 t.vip,
 t.contrato,
 t.carteira,
 t.data_nascimento,
 t.tipo,
 t.numerodocumento,
 t.datavencimento,
 t.valordocumento,
 t.datapagamento,
 t.valorpago,
 t.valordesconto,
 t.saldo,
 t.linhadigitavel,
 t.tipopagamento,
 t.localcobranca,
 t.tipocontrato,
 t.cpf_cnpj,
 t.classificao,
 translate(t.telefone, '.,-()', ''),
 t.vencimentocontrato,
 t.situacaocontrato,
 t.periodofatura,
 t.telefone1,
 t.telefone2,
 t.telefone3,
 t.idcontrato,
 t.bairro,
 t.tipo_logradouro,
 t.logradouro,
 t.numero_conexao,
 t.cep_conexao,
 t.faturaimpressa,
 t.enviarporemail
from regrasoperacao.vis_boletos_em_aberto_geral t
where t.datavencimento < current_date and t.carteira in ('CONEXÃO')

--------------------------------------------------------------------------------------------------------------------------------------------

--RELATÓRIO ARTHUR MASTER
with
x as(
SELECT distinct cid.nomedacidade,
       cli.codigocliente,
       ct.contrato,
       cli.nome,
       ord.d_dataatendimento as cancelamento,
       ord.d_dataexecucao as corte_sinal,
       ord.numos,
       B.DESCRICAODOSERV_LANC as servico_executado
FROM ordemservico ORD
     JOIN cidade cid ON cid.codigodacidade = ord.cidade
     JOIN lanceservicos lr ON lr.codigodoserv_lanc = ord.codservsolicitado
     JOIN clientes cli ON cli.cidade = ord.cidade and cli.codigocliente = ord.codigoassinante
     JOIN contratos ct ON ct.cidade = ord.cidade and ct.codempresa = ord.codempresa and ct.contrato = ord.codigocontrato
     LEFT JOIN SERVEXECUTADOSOS se ON se.cidade = ord.cidade and se.codempresa = ord.codempresa and se.numos = ord.numos
     LEFT JOIN LANCESERVICOS B ON se.CODIGOSERVICO = B.CODIGODOSERV_LANC
WHERE lr.codigodoserv_lanc IN (181)
),
y as (
SELECT distinct cid.nomedacidade,
       cli.codigocliente,
       ct.contrato,
       cli.nome,
       max(ord.d_dataexecucao) as retirada,
       ord.numos,
       B.DESCRICAODOSERV_LANC as servico_executado,
       array_agg(split_part(pro.descricao, '/'::text, 1)) as material_recolhido
FROM ordemservico ORD
     JOIN cidade cid ON cid.codigodacidade = ord.cidade
     JOIN lanceservicos lr ON lr.codigodoserv_lanc = ord.codservsolicitado
     JOIN clientes cli ON cli.cidade = ord.cidade and cli.codigocliente = ord.codigoassinante
     JOIN contratos ct ON ct.cidade = ord.cidade and ct.codempresa = ord.codempresa and ct.contrato = ord.codigocontrato
     LEFT JOIN MATERIAISOSRETIRADA mr ON mr.codigocidade = ord.cidade and mr.codempresa = ord.codempresa and mr.numos = ord.numos
     LEFT JOIN PRODUTOS pro ON pro.codigo = mr.codigomaterial
     LEFT JOIN SERVEXECUTADOSOS se ON se.cidade = ord.cidade and se.codempresa = ord.codempresa and se.numos = ord.numos
     LEFT JOIN LANCESERVICOS B ON se.CODIGOSERVICO = B.CODIGODOSERV_LANC
WHERE lr.codigodoserv_lanc IN (611) and
      b.descricaodoserv_lanc ilike '%RETIRADA%'
GROUP BY 1,2,3,4,6,7
),
z as (
SELECT a.nomedacidade,
       a.codigocliente,
       a.contrato,
       a.nome,
       a.numos,
       CASE WHEN a.ignoravalor = 0 THEN a.multa ELSE 0 END AS multa,
       a.servico_executado
FROM (
      SELECT distinct cid.nomedacidade,
                      cli.codigocliente,
                      ct.contrato,
                      cli.nome,
                      ord.numos,
                      se.ignoravalor,
                      sum(se.valorservico) as multa,
                      array_agg(split_part(B.DESCRICAODOSERV_LANC, '/'::text, 1)) as servico_executado
      FROM ordemservico ORD
      JOIN cidade cid ON cid.codigodacidade = ord.cidade
      JOIN lanceservicos lr ON lr.codigodoserv_lanc = ord.codservsolicitado
      JOIN clientes cli ON cli.cidade = ord.cidade and cli.codigocliente = ord.codigoassinante
      JOIN contratos ct ON ct.cidade = ord.cidade and ct.codempresa = ord.codempresa and ct.contrato = ord.codigocontrato
      LEFT JOIN MATERIAISOSRETIRADA mr ON mr.codigocidade = ord.cidade and mr.codempresa = ord.codempresa and mr.numos = ord.numos
      LEFT JOIN SERVEXECUTADOSOS se ON se.cidade = ord.cidade and se.codempresa = ord.codempresa and se.numos = ord.numos
      LEFT JOIN LANCESERVICOS B ON se.CODIGOSERVICO = B.CODIGODOSERV_LANC
      WHERE lr.codigodoserv_lanc IN (611) and
            b.descricaodoserv_lanc ilike '%MULTA%'
      GROUP BY 1,2,3,4,5,6) as a
)
SELECT cid.nomedacidade,
       cli.codigocliente,
       ct.contrato,
       cli.nome,
       x.cancelamento,
       x.corte_sinal,
       y.retirada,
       CASE WHEN z.servico_executado is null THEN 'NÃO' ELSE 'SIM' END AS cobroumulta,
       z.multa,
       CASE WHEN dr.fatura is not null THEN z.multa ELSE 0 END AS multafaturada,
       CASE WHEN dr.fatura is null THEN z.multa ELSE 0 END AS multapendente,
       dr.fatura,
       dr.d_datavencimento,
       CASE WHEN dr.situacao = 1 THEN 'Cancelado' ELSE 'Normal' END AS statusfatura,
       y.material_recolhido
       
FROM ordemservico ord
     JOIN contratos ct ON ct.cidade = ord.cidade and ct.codempresa = ord.codempresa and ct.contrato = ord.codigocontrato
     JOIN cidade cid ON cid.codigodacidade = ord.cidade
     JOIN clientes cli ON cli.cidade = ord.cidade and cli.codigocliente = ord.codigoassinante
     LEFT JOIN docreceber dr ON dr.cliente = ct.codigodocliente and dr.codigodacidade = ct.cidade
     JOIN x ON x.nomedacidade = cid.nomedacidade and x.codigocliente = cli.codigocliente and x.contrato = ct.contrato and x.nome = cli.nome
     JOIN y ON y.nomedacidade = cid.nomedacidade and y.codigocliente = cli.codigocliente and y.contrato = ct.contrato and y.nome = cli.nome
     JOIN z ON z.nomedacidade = cid.nomedacidade and z.codigocliente = cli.codigocliente and z.contrato = ct.contrato and z.nome = cli.nome
WHERE ord.d_dataexecucao BETWEEN '2022-01-01' AND to_char(current_date + 32, 'YYYY-MM-01'::text)::date - 1 and
      ord.d_dataagendamento BETWEEN '2022-01-01' AND to_char(current_date + 32, 'YYYY-MM-01'::text)::date - 1
GROUP BY cid.nomedacidade,
         cli.codigocliente,
         ct.contrato,
         cli.nome,
         x.cancelamento,
         x.corte_sinal,
         y.retirada,
         cobroumulta,
         z.multa,
         dr.fatura,
         dr.d_datavencimento,
         statusfatura,
         y.material_recolhido

--------------------------------------------------------------------------------------------------------------------------------------------

--RELATÓRIO MASTER ARTHUR - 28055
with
cancelamento as(
SELECT ct.id, MAX(ord.d_dataexecucao) as maior_data_cancelamento
FROM ordemservico ORD
JOIN lanceservicos lr ON lr.codigodoserv_lanc = ord.codservsolicitado
JOIN contratos ct ON ct.cidade = ord.cidade and ct.codempresa = ord.codempresa and ct.contrato = ord.codigocontrato
where lr.situacaocontrato = 5 and ct.situacao = 5
group by ct.id
),
retirada as (
SELECT ct.id, max(ord.d_dataexecucao) as maior_data_retirada, array_agg(split_part(pro.descricao, '[', 1)) as material_recolhido
FROM ordemservico ORD
JOIN lanceservicos lr ON lr.codigodoserv_lanc = ord.codservsolicitado
JOIN contratos ct ON ct.cidade = ord.cidade and ct.codempresa = ord.codempresa and ct.contrato = ord.codigocontrato
JOIN MATERIAISOSRETIRADA mr ON mr.codigocidade = ord.cidade and mr.codempresa = ord.codempresa and mr.numos = ord.numos
JOIN PRODUTOS pro ON pro.codigo = mr.codigomaterial
WHERE lr.codigodoserv_lanc IN (611, 181, 2431)
GROUP BY ct.id
),
multa as (
select ct.id, dr.d_datavencimento, dr.valordocumento, array_agg(split_part(m.observacao, '/'::text, 1)) as multa_lancada
from movimfinanceiro m
join contratos ct on ct.cidade=m.cidade and ct.codempresa=m.codempresa and ct.contrato=m.contrato
left join docreceber dr on dr.fatura=m.numfatura
where m.observacao ilike '%MULTA INDEN%' and dr.situacao = 0
group by ct.id, dr.d_datavencimento, dr.valordocumento
)
SELECT distinct cid.nomedacidade, cli.codigocliente, cli.nome, ct.contrato, c.maior_data_cancelamento, r.maior_data_retirada, m.*
FROM contratos ct
JOIN clientes cli ON cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
JOIN cidade cid ON cid.codigodacidade = ct.cidade
JOIN cancelamento c on c.id = ct.id
left join multa m on m.id=ct.id
join retirada r on r.id=ct.id
WHERE (r.maior_data_retirada - c.maior_data_cancelamento > 30)

--------------------------------------------------------------------------------------------------------------------------------------------

-- ESTORNAR AQUIVO DO BANCO -- verificar quantidade 
select dr.nomedoarquivoquebaixou, dr.numerodocumento, dr.*
from docreceber dr
where dr.nomedoarquivoquebaixou ilike '%CB200701%';  --818


-- PASSO 1 - Quando o processo chega no Boletos baixados -- se retornar vazio, ir para o passo 2
delete from movimfinanceiro where id in (
  select m.id, m.observacao, m.usuario, m.d_datacadastro, m.t_horacadastro, b.nomearquivo
  from boletosbaixados b 
  join docreceber dr on dr.nossonumero::text=b.nossonumero
  join movimfinanceiro m on m.cidade=dr.codigodacidade and m.assinante=dr.cliente and m.d_datacadastro between '2022-07-20' and '2022-07-20' 
  --and lower(m.usuario) in ('igormonteiro')
  and m.numfatura is null 
  where b.nomearquivo ilike '%CB200701%' --and  b.situacao = 'Crédito/Baixado Anteriormente!'
--  and dr.codigodacidade <> 329721
  --and dr.cliente = 108051
);

update docreceber set d_datapagamento = null, valorpago = 0 where id in (
  select dr.id--, dr.d_dataliquidacao, dr.d_datapagamento
  from docreceber dr
  where dr.nomedoarquivoquebaixou ilike '%CB120700.RET%' and dr.d_dataliquidacao = '2022-07-12' and d_datapagamento is not null
);


delete from boletosbaixados where id in (
  select b.id
  from boletosbaixados b 
  where b.nomearquivo ilike '%CB190500.RET%' and b.usuario = 'igormonteiro' and b.d_data >= '2022-05-19'
);

--PASSO 2 - Quando o processo de baixa não chega no Boletos Baixados
select dr.dataaud, dr.loginaud, dr.nomedoarquivoquebaixou, count(*)

from auditoria.aud_docreceber dr
where dr.nomedoarquivoquebaixou ilike '%CB200701%'
--and lower(dr.loginaud) in ('igormonteiro','postgres')
and dr.dataaud::date = '2022-07-20'
 --and dr.codigodacidade = 329721 
--and dr.dataaud between '2022-04-22 13:59' and '2022-04-22 15:55'
group by dr.dataaud, dr.loginaud, dr.nomedoarquivoquebaixou

  
delete from movimfinanceiro where id in ( 
  select m.id--, m.observacao, m.usuario, m.t_horacadastro
  from movimfinanceiro m
  where /*m.cidade = 329721 and*/  m.d_datacadastro = '2022-07-20' and lower(m.usuario) in ('linniabarreto','postgres')
  and m.t_horacadastro between '08:46' and '08:52'  
);

update docreceber set d_datapagamento = null, valorpago = 0 where id in (
  select dr.id--, dr.d_dataliquidacao, dr.d_datapagamento
  from docreceber dr
  where dr.nomedoarquivoquebaixou ilike '%CB200701%' and dr.d_dataliquidacao = '2022-07-20' and d_datapagamento is not null
);

--------------------------------------------------------------------------------------------------------------------------------------------

--FUNÇÃO RELATÓRIO PERSONALIZADO CSP TÉCNICA ORDENS GERAL
    BEGIN
    	Create temporary table temp_rp_csp_tecnica_ordens_geral(
          "Carteira" varchar(50),
          "Nome-Cidade" varchar(30),
          "Codigo-Assinante" integer,
          "Descricao" varchar(40),
          "Contrato" integer,
          "Situcao Contrato" text,
          "Num-OS" bigint,
          "Serviço" varchar(40),
          "Data-Atendimento" date,
          "Hora-Atendimento" text,
          "Data-Agendamento" date,
          "Data-Execução" date,
          "Hora-Execução" text,
          "Data-Baixa" date,
          "Bairro" varchar(20),
          "Equipe" varchar(30),
          "Equipe-Executou" varchar(30),
          "Tipo-OS" text,
          "Motivo-Cancelamento" varchar(50),
          "Usuario" varchar(20),
          "Vendedor" varchar(40),
          "Pacote" text,
          "Valor_Pacote" text,
          "Endereco" text,
          "Num-Conexão" varchar(10),
          "Apto-Conexão" varchar(10),
          "Bloco-Conexão" varchar(10),
          "Bairro-Conexão" varchar(20),
          "id_Contrato" bigint,
          "Ocorrencias" text
		) On commit drop;
        
        pcarteira := lower(to_ascii(pcarteira::varchar));
        pempresa := lower(to_ascii(pempresa::varchar));
        
        insert into temp_rp_csp_tecnica_ordens_geral
          select distinct os.carteira as "Carteira", 
              os.nomecidade as "Cidade", 
              os.codigoassinante as "Código", 
              os.nome as "Nome Cliente", 
              os.codigocontrato as "Contrato",
              os.descricaosituacao as "Situação", 
              os.numos as "Nº OS", 
              os.servico as "Serviço", 
              os.data_atendimento as "Atendimento", 
              to_char(os.horaatendimento,'HH24:MM') as "Hora Atendimento",
              os.data_agendameanto as "Agendamento", 
              os.data_execucao as "Execução", 
              to_char(os.horaexecucao,'HH24:MM') as "Hora Execução",
              os.data_realbaixa as "Data Real Baixa", 
              os.bairro as "Bairro", 
              os.equipe as "Equipe", 
              os.equipeexecutou as "Equipe executou", 
              os.tipo as "Tipo",
              os.motivo_cancelamento as "Motivo Cancelamento", 
              os.usuario_abriu as "Usuario que Abriu", 
              os.vendedor as "Vendedor", 
              os.pacote as "Pacote", 
              os.valor_pacote as "Valor_Pacote",
              os.endereco as "Endereço", 
              os.numeroconexao as "Nº Conexão", 
              os.aptoconexao as "Apto", 
              os.blococonexao as "Bloco", 
              os.bairroconexao as "Bairro", 
              os.idcontrato,
              os.ocorrencias as "Ocorrências"
          from regrasoperacao.vis_ordem_servico_planos_conexao os 
          where os.data_agendameanto is not null  and os.data_atendimento between pdatainicial and pdatafinal 
          and lower(to_ascii(os.carteira::varchar)) ilike pcarteira  
          and lower(to_ascii(os.empresa::varchar)) ilike pempresa  ;
           
        return query select * from temp_rp_csp_tecnica_ordens_geral;
           
    end;

--------------------------------------------------------------------------------------------------------------------------------------------

 -- relatorio prospect pra conexao
select distinct a.id, a.nome, t.nome, date(a.criadoem) as datacriacao, u.nome as vendedor, ca.descricao as canalvenda, btrim(s.descricao) as situacao, an.nome as analista,
pt.vis_nome_pacote, pt.vis_valor, ta.codigo, ta.nomedatabeladeprecos
from interfocusprospect.assinatura a
join interfocusprospect.usuariolocal u on u.id=a.captador
join interfocusprospect.statusassinatura s on s.id=a.statusassinatura
join public.tablocal t on t.id=a.municipioterceirosconexao
left join interfocusprospect.usuariolocal an on an.id=a.analistaid
join public.vendedores v on v.id=u.vendedorterceiros
join public.canaisdevenda ca on ca.cidade=v.cidadeondetrabalha and ca.codigo=v.canalvenda
join interfocusprospect.assinaturapacoteterceiros ap on ap.assinatura=a.id
JOIN interfocusprospect.vis_pacotetabela pt ON pt.vis_id =ap.pacoteterceiros
join tabeladeprecos ta on ta.id=pt.vis_id_tabela_preco
where date(a.criadoem) between '2022-02-01' and '2022-02-28'

--------------------------------------------------------------------------------------------------------------------------------------------

--RELATORIO VENDAS PROSPECT 
WITH status AS(
  SELECT aa.assinatura,
         max(aa.criadoem)::date AS dataalteracao
  FROM interfocusprospect.assinaturaandamento aa
  WHERE aa.statusassinaturaatual = ANY (ARRAY [ 3::bigint, 12::bigint ])
  GROUP BY aa.assinatura)
    SELECT a.id,
           a.nome,
           a.cpfcnpj AS cpf_cnpj,
           c.vis_nome AS cidade,
           sa.descricao AS status_assinatura,
           cap.nome AS vendedor,
           ul.nome AS analista,
           a.criadoem::date,
           CASE
             WHEN (a.statusassinatura = ANY (ARRAY [ 3::bigint, 12::bigint ]))
               AND s.assinatura IS NOT NULL AND s.dataalteracao IS NOT NULL THEN
               s.dataalteracao
             WHEN (a.statusassinatura = ANY (ARRAY [ 3::bigint, 12::bigint ]))
               AND s.assinatura IS NOT NULL AND s.dataalteracao IS NULL THEN
               a.ultimaalteracao::date
             WHEN (a.statusassinatura = ANY (ARRAY [ 3::bigint, 12::bigint ]))
               AND date (a.dataprocessamento) IS NULL THEN a.ultimaalteracao::
               date
             ELSE date (a.dataprocessamento)
           END AS data_processamento,
           (
             SELECT translate(aa.observacao, ';'::text, ' '::text) AS translate
             FROM interfocusprospect.assinaturaandamento aa
             WHERE aa.assinatura = a.id
             ORDER BY aa.id DESC
             LIMIT 1
           ) AS observacao,
           (
             SELECT ca_1.descricao
             FROM interfocusprospect.assinaturaandamento aa
                  LEFT JOIN interfocusprospect.classificacaoandamento ca_1 ON
                    ca_1.id = aa.classificacaoandamento
             WHERE aa.assinatura = a.id
             ORDER BY aa.id DESC
             LIMIT 1
           ) AS classificacao,
           (
             SELECT ev.descricao
             FROM vendedores v
                  JOIN equipesdevenda ev ON ev.cidade = v.cidadeondetrabalha AND
                    ev.codigo = v.equipevenda
             WHERE v.id = cap.vendedorterceiros
           ) AS equipe_venda,
           date (a.nascimentoabertura) AS data_nascimento,
           ct.id AS id_contrato,
           tc.vis_descricao AS tipo_contrato,
           tv.vis_descricao AS tipo_captacao,
           ct.d_datadainstalacao AS data_instalacao,
           a.carteiraterceiros AS codigo_carteira,
           ca.descricao AS carteira,
         /*  (
             SELECT p.vis_id_pacote
             FROM interfocusprospect.assinaturapacoteterceiros aa
                  JOIN interfocusprospect.vis_pacotetabela p ON p.vis_id =
                    aa.pacoteterceiros
         --    WHERE aa.assinatura = a.id
           ) AS id_pacote,*/
           array_to_string(ARRAY
           (
             SELECT p.vis_nome_pacote::text AS vis_nome_pacote
             FROM interfocusprospect.assinaturapacoteterceiros aa
                  JOIN interfocusprospect.vis_pacotetabela p ON p.vis_id =
                    aa.pacoteterceiros
             WHERE aa.assinatura = a.id
           ), ','::text, ''::text) AS pacotes,
           array_to_string(ARRAY
           (
             SELECT sum(p.vis_valor) AS valor
             FROM interfocusprospect.assinaturapacoteterceiros aaa
                  JOIN interfocusprospect.vis_pacotetabela p ON p.vis_id =
                    aaa.pacoteterceiros
             WHERE aaa.assinatura = a.id
           ), ''::text, ''::text) AS pacotes_valores
    FROM interfocusprospect.assinatura a
         JOIN interfocusprospect.statusassinatura sa ON sa.id =
           a.statusassinatura
         LEFT JOIN interfocusprospect.usuariolocal ul ON ul.id = a.analistaid
         JOIN interfocusprospect.usuariolocal cap ON cap.id = a.captador
         JOIN interfocusprospect.vis_cidade c ON c.vis_id =
           a.municipioterceirosconexao
         LEFT JOIN interfocusprospect.vis_tipocontrato tc ON tc.vis_id =
           a.tipocontratoterceiros
         LEFT JOIN interfocusprospect.vis_tiposdevenda tv ON tv.vis_id =
           a.tipocaptacao
         LEFT JOIN contratos ct ON ct.id = a.contratoid
         LEFT JOIN carteira ca ON ca.id = a.carteiraterceiros
         LEFT JOIN status s ON s.assinatura = a.id
         where a.criadoem::date  between '2022-07-19'::date and '2022-07-21'::date

--------------------------------------------------------------------------------------------------------------------------------------------

--FUNÇÃO RELATÓRIO GERAL HISTORICOS GERAIS
CREATE OR REPLACE FUNCTION relatoriospersonalizados.func_rp_grp_cnx_historicos_gerais (
  pdatainicial date,
  pdatafinal date,
  pcarteiras bigint []
)
RETURNS TABLE (
  "CONTRATO" integer,
  "CIDADE" varchar,
  "CÓDIGO_ASSI" integer,
  "NOME" varchar,
  "CPF/CNPJ" varchar,
  "TIPO_HISTORICO" text,
  "PROTOCOLO" text,
  "HISTORICO_PAI" integer,
  "ATENDENTE" varchar,
  "DATA_CADASTRO" date,
  "HORA_CADASTRO" time,
  "DATA_FECHAMENTO" date,
  "HORA_FECHAMENTO" time,
  "TEMPO_ATENDIMENTO" time,
  "GRUPO" varchar,
  "ASSUNTO" varchar,
  "TELEFONE" text,
  "SITUAÇÃO_CONTRATO" text,
  "STATUS" text,
  "RAZAO_SOCIAL" varchar,
  "CARTEIRA" varchar,
  "ID_CONTRATO" integer,
  "SITUAÇÃO_ASSUNTO" varchar
) AS
$body$
      BEGIN
    	Create temporary table temp_rp_grp_cnx_historicos_gerais(
            "CONTRATO" integer,
            "CIDADE" varchar(30),
            "CÓDIGO_ASSI" integer,
            "NOME" varchar (40),
            "CPF/CNPJ" varchar(18),
            "TIPO_HISTORICO" text,
            "PROTOCOLO" text,
            "HISTORICO_PAI" integer,
            "ATENDENTE" varchar(20),
            "DATA_CADASTRO" date,
            "HORA_CADASTRO" time,
            "DATA_FECHAMENTO" date,
            "HORA_FECHAMENTO" time,
            "TEMPO_ATENDIMENTO" time,
            "GRUPO" varchar(30),
            "ASSUNTO" varchar(50),
            "TELEFONE" text,
            "SITUAÇÃO_CONTRATO" text,
            "STATUS" text,
            "RAZAO_SOCIAL" varchar(50),
            "CARTEIRA" varchar(50),
            "ID_CONTRATO" integer,
            "SITUAÇÃO_ASSUNTO" varchar(50)
		) On commit drop;
       
        insert into temp_rp_grp_cnx_historicos_gerais
          with
            s as (
                select hg.id, 
                    CASE WHEN hg.d_datafechamento IS NOT NULL THEN 1
                         WHEN hg.d_datafechamento IS NULL AND hpai.d_datafechamento IS NOT NULL THEN  1 
                         ELSE 2
                    END AS status 
                from historicogeral hg
                LEFT JOIN historicogeral hpai ON hpai.controle = hg.historicopai 
          )
          select distinct ct.contrato as "CONTRATO",
                          ci.nomedacidade AS "CIDADE",
                          cli.codigocliente AS "CÓDIGO_ASSI",
                          cli.nome AS "NOME",
                          cli.cpf_cnpj AS "CPF/CNPJ", 
                          case when hg.historicopai is null then 'Principal' else 'Andamento' end as "TIPO_HISTORICO",
                          hg.controle AS "PROTOCOLO", 
                          hg.historicopai AS "HISTORICO_PAI", 
                          hg.atendente AS "ATENDENTE",
                          hg.d_datacadastro AS "DATA_CADASTRO",
                          hg.t_horacadastro AS "HORA_CADASTRO", 
                          hg.d_datafechamento AS "DATA_FECHAMENTO",
                          hg.t_horafechamento AS "HORA_FECHAMENTO",
                          
          CASE WHEN hg.d_datafechamento IS NOT NULL THEN (((hg.d_datafechamento || ' '::text) || hg.t_horafechamento)::timestamp without time zone) -(((
          hg.d_data || ' '::text) || hg.t_hora)::timestamp without time zone)
            WHEN hg.d_datafechamento IS NULL AND hpai.d_datafechamento IS NOT NULL THEN (((hpai.d_datafechamento || ' '::text) ||
              hpai.t_horafechamento)::timestamp without time zone) -(((hpai.d_data || ' '::text) || hpai.t_hora)::timestamp without time zone)
            ELSE NULL::interval
          END AS "TEMPO_ATENDIMENTO", 
            translate(g.descricao,'.-;:,',',') as "GRUPO",
            translate(a.descricao ,'.-:;,',',') as "ASSUNTO",
            func_retornatelefones(ct.cidade, ct.codigodocliente) as "TELEFONE", 
            v.descricaosituacao AS "SITUAÇÃO_CONTRATO", 
            case when  s.status = 1 then 'fechado' else 'aberto' end as "STATUS",
            e.razaosocial as "RAZAO_SOCIAL",
            ca.descricao as "CARTEIRA",
            ct.id as "ID_CONTRATO",
            t.descricao as "SITUAÇÃO_ASSUNTO"
          from historicogeral hg 
          join contratos ct on ct.cidade = hg.codigocidade and ct.codempresa = hg.codempresa and ct.contrato = hg.codcontrato
          join clientes cli on cli.cidade=ct.cidade and cli.codigocliente=ct.codigodocliente
          join cidade ci on ci.codigodacidade=ct.cidade 
          join empresas e on e.codcidade = ct.cidade and e.codempresa = ct.codempresa
          LEFT JOIN historicogeral hpai ON hpai.controle = hg.historicopai
          JOIN assuntohistorico a ON a.codigogrupo = hg.grupoassunto AND a.codigoassunto = hg.assunto
          JOIN grupohistorico g ON g.codigo = hg.grupoassunto
          LEFT JOIN usuariosdohistorico u ON u.controlehistorico = hg.controle
          LEFT JOIN hwusers hu ON lower(hu.login::text) = lower(u.usuario::text)
          LEFT JOIN hwgroups hgr ON hgr.id = hu.groupid
          LEFT JOIN hwusers hua ON lower(hua.login::text) = lower(hg.atendente::text )
          LEFT JOIN hwgroups hga ON hga.id = hua.groupid
          LEFT JOIN tiposituacaohistorico t ON t.codigo = hg.codigotiposituacao
          join vis_situacaocontrato_descricao v on v.situacao=ct.situacao
          join carteira ca on ca.codigo=ct.codcarteira
          join s on s.id = hg.id 
        where hg.d_datacadastro BETWEEN pdatainicial and pdatafinal and
        ct.codcarteira = any(pCarteiras);
                             
        return query select * from temp_rp_grp_cnx_historicos_gerais;
		                 
    end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100 ROWS 1000;

ALTER FUNCTION relatoriospersonalizados.func_rp_grp_cnx_historicos_gerais (pdatainicial date, pdatafinal date, pcarteiras bigint [])
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

-- VIEW RELATÓRIO GERAL - HISTORICOS GERAIS
CREATE VIEW regrasoperacao.vis_geral_historicos_gerais (
contrato,
cidade,
código_assi,
nome,
cpf_cnpj,
tipo_historico,
protocolo,
historico_pai,
atendente,
data_cadastro,
hora_cadastro,
data_fechamento,
hora_fechamento,
tempo_atendimento,
grupo,
assunto,
telefone,
situação_contrato,
status,
razao_social,
carteira,
id_contrato,
situação_assunto
)
AS
with
  s as (
      select hg.id, 
          CASE WHEN hg.d_datafechamento IS NOT NULL THEN 1
               WHEN hg.d_datafechamento IS NULL AND hpai.d_datafechamento IS NOT NULL THEN  1 
               ELSE 2
          END AS status 
      from historicogeral hg
      LEFT JOIN historicogeral hpai ON hpai.controle = hg.historicopai 
)
select distinct ct.contrato as "CONTRATO",
                ci.nomedacidade AS "CIDADE",
                cli.codigocliente AS "CÓDIGO_ASSI",
                cli.nome AS "NOME",
                cli.cpf_cnpj AS "CPF/CNPJ", 
                case when hg.historicopai is null then 'Principal' else 'Andamento' end as "TIPO_HISTORICO",
                hg.controle AS "PROTOCOLO", 
                hg.historicopai AS "HISTORICO_PAI", 
                hg.atendente AS "ATENDENTE",
                hg.d_datacadastro AS "DATA_CADASTRO",
                hg.t_horacadastro AS "HORA_CADASTRO", 
                hg.d_datafechamento AS "DATA_FECHAMENTO",
                hg.t_horafechamento AS "HORA_FECHAMENTO",
                          
CASE WHEN hg.d_datafechamento IS NOT NULL THEN (((hg.d_datafechamento || ' '::text) || hg.t_horafechamento)::timestamp without time zone) -(((
hg.d_data || ' '::text) || hg.t_hora)::timestamp without time zone)
  WHEN hg.d_datafechamento IS NULL AND hpai.d_datafechamento IS NOT NULL THEN (((hpai.d_datafechamento || ' '::text) ||
    hpai.t_horafechamento)::timestamp without time zone) -(((hpai.d_data || ' '::text) || hpai.t_hora)::timestamp without time zone)
  ELSE NULL::interval
END AS "TEMPO_ATENDIMENTO", 
  translate(g.descricao,'.-;:,',',') as "GRUPO",
  translate(a.descricao ,'.-:;,',',') as "ASSUNTO",
  func_retornatelefones(ct.cidade, ct.codigodocliente) as "TELEFONE", 
  v.descricaosituacao AS "SITUAÇÃO_CONTRATO", 
  case when  s.status = 1 then 'fechado' else 'aberto' end as "STATUS",
  e.razaosocial as "RAZAO_SOCIAL",
  ca.descricao as "CARTEIRA",
  ct.id as "ID_CONTRATO",
  t.descricao as "SITUAÇÃO_ASSUNTO"
from historicogeral hg 
join contratos ct on ct.cidade = hg.codigocidade and ct.codempresa = hg.codempresa and ct.contrato = hg.codcontrato
join clientes cli on cli.cidade=ct.cidade and cli.codigocliente=ct.codigodocliente
join cidade ci on ci.codigodacidade=ct.cidade 
join empresas e on e.codcidade = ct.cidade and e.codempresa = ct.codempresa
LEFT JOIN historicogeral hpai ON hpai.controle = hg.historicopai
JOIN assuntohistorico a ON a.codigogrupo = hg.grupoassunto AND a.codigoassunto = hg.assunto
JOIN grupohistorico g ON g.codigo = hg.grupoassunto
LEFT JOIN usuariosdohistorico u ON u.controlehistorico = hg.controle
LEFT JOIN hwusers hu ON lower(hu.login::text) = lower(u.usuario::text)
LEFT JOIN hwgroups hgr ON hgr.id = hu.groupid
LEFT JOIN hwusers hua ON lower(hua.login::text) = lower(hg.atendente::text )
LEFT JOIN hwgroups hga ON hga.id = hua.groupid
LEFT JOIN tiposituacaohistorico t ON t.codigo = hg.codigotiposituacao
join vis_situacaocontrato_descricao v on v.situacao=ct.situacao
join carteira ca on ca.codigo=ct.codcarteira
join s on s.id = hg.id;

ALTER VIEW regrasoperacao.vis_geral_historicos_gerais
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

-- FUNÇÃO RELATÓRIO GERAL HISTORICOS GERAIS - COM FILTRO
CREATE OR REPLACE FUNCTION relatoriospersonalizados.func_rp_grl_historicos_gerais (
  pdatainicial date,
  pdatafinal date,
  phistorico text
)
RETURNS TABLE (
  "CONTRATO" integer,
  "CIDADE" varchar,
  "CÓDIGO_ASSI" integer,
  "NOME" varchar,
  "CPF/CNPJ" varchar,
  "TIPO_HISTORICO" text,
  "PROTOCOLO" text,
  "HISTORICO_PAI" integer,
  "ATENDENTE" varchar,
  "DATA_CADASTRO" date,
  "HORA_CADASTRO" time,
  "DATA_FECHAMENTO" date,
  "HORA_FECHAMENTO" time,
  "TEMPO_ATENDIMENTO" time,
  "GRUPO" varchar,
  "ASSUNTO" varchar,
  "TELEFONE" text,
  "SITUAÇÃO_CONTRATO" text,
  "STATUS" text,
  "RAZAO_SOCIAL" varchar,
  "CARTEIRA" varchar,
  "ID_CONTRATO" integer,
  "SITUAÇÃO_ASSUNTO" varchar
) AS
$body$
      BEGIN
    	Create temporary table temp_rp_grl_historicos_gerais(
            "CONTRATO" integer,
            "CIDADE" varchar(30),
            "CÓDIGO_ASSI" integer,
            "NOME" varchar (40),
            "CPF/CNPJ" varchar(18),
            "TIPO_HISTORICO" text,
            "PROTOCOLO" text,
            "HISTORICO_PAI" integer,
            "ATENDENTE" varchar(20),
            "DATA_CADASTRO" date,
            "HORA_CADASTRO" time,
            "DATA_FECHAMENTO" date,
            "HORA_FECHAMENTO" time,
            "TEMPO_ATENDIMENTO" time,
            "GRUPO" varchar(30),
            "ASSUNTO" varchar(50),
            "TELEFONE" text,
            "SITUAÇÃO_CONTRATO" text,
            "STATUS" text,
            "RAZAO_SOCIAL" varchar(50),
            "CARTEIRA" varchar(50),
            "ID_CONTRATO" integer,
            "SITUAÇÃO_ASSUNTO" varchar(50)
		) On commit drop;
       
      		phistorico := lower(to_ascii(phistorico::text)) || '%';
      
        insert into temp_rp_grl_historicos_gerais
        	select hg.contrato,
                   hg.cidade,
                   hg.código_assi,
                   hg.nome,
                   hg.cpf_cnpj,
                   hg.tipo_historico,
                   hg.protocolo,
                   hg.historico_pai,
                   hg.atendente,
                   hg.data_cadastro,
                   hg.hora_cadastro,
                   hg.data_fechamento,
                   hg.hora_fechamento,
                   hg.tempo_atendimento,
                   hg.grupo,
                   hg.assunto,
                   hg.telefone,
                   hg.situação_contrato,
                   hg.status,
                   hg.razao_social,
                   hg.carteira,
                   hg.id_contrato,
                   hg.situação_assunto
            from regrasoperacao.vis_geral_historicos_gerais hg
        where hg.data_cadastro BETWEEN pdatainicial and pdatafinal and 
        lower(to_ascii(hg.status::text)) ilike phistorico;
                             
        return query select * from temp_rp_grl_historicos_gerais;
		                 
    end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100 ROWS 1000;

ALTER FUNCTION relatoriospersonalizados.func_rp_grl_historicos_gerais (pdatainicial date, pdatafinal date, phistorico text)
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

-- ENDEREÇO CONCATENADO
(ed.tipodologradouro::text || ' '::text) || ed.nomelogradouro::text AS "Endereço"

--------------------------------------------------------------------------------------------------------------------------------------------

-- TIPO TECNOLOGIA
func_retorna_tipotecnologia(ct.cidade, ct.codempresa, ct.contrato) as "Tecnologia"

--------------------------------------------------------------------------------------------------------------------------------------------

-- CRIAR SCHEMA relatorios personalizados
CREATE SCHEMA relatoriospersonalizados AUTHORIZATION postgres;

ALTER SCHEMA relatoriospersonalizados
  OWNER TO postgres;
  
--------------------------------------------------------------------------------------------------------------------------------------------
  
-- VIEW ANALITICO PRIMEIRA CONEXÃO
CREATE OR REPLACE VIEW regrasoperacao.vis_analitico_primeira_conexao(
    regional,
    cod_cidade,
    cidade,
    "código_serviço",
    "serviço",
    num_os,
    data_atendimento,
    data_agendamento,
    "data_execução",
    "código_assinante",
    nome_assinante,
    contrato,
    "endereço",
    "número",
    complemento,
    bairro,
    cep,
    tecnologia,
    cod_motivo_cancelamento,
    vendedor,
    equipe_venda,
    situacao_contrato)
AS
  SELECT reg.descricao AS regional,
         cid.codigodacidade AS cod_cidade,
         cid.nomedacidade AS cidade,
         ord.codservsolicitado AS "código_serviço",
         l.descricaodoserv_lanc AS "serviço",
         ord.numos AS num_os,
         ord.d_dataatendimento AS data_atendimento,
         ord.d_dataagendamento AS data_agendamento,
         ord.d_dataexecucao AS "data_execução",
         cli.codigocliente AS "código_assinante",
         cli.nome AS nome_assinante,
         ct.contrato,
         (ed.tipodologradouro::text || ' '::text) || ed.nomelogradouro::text AS "endereço",
         ct.numeroconexao AS "número",
         ct.complementoconexao AS complemento,
         ct.bairroconexao AS bairro,
         ct.cepconexao AS cep,
         func_retorna_tipotecnologia(ct.cidade, ct.codempresa, ct.contrato) AS tecnologia,
         ord.motivocancelamento AS cod_motivo_cancelamento,
         v.nome AS vendedor,
         ev.descricao AS equipe_venda,
         CASE
           WHEN ct.situacao = 1 THEN 'AGUARDANDO CONEXÃO'::text
           WHEN ct.situacao = 2 THEN 'CONECTADO'::text
           WHEN ct.situacao = 3 THEN 'PAUSADO'::text
           WHEN ct.situacao = 4 THEN 'INADIMPLENTE'::text
           WHEN ct.situacao = 5 THEN 'CANCELADO'::text
           WHEN ct.situacao = 6 THEN 'SEM CABEAMENTO'::text
           WHEN ct.situacao = 7 THEN 'CONECTADO/INADIMPLENTE'::text
           ELSE 'Outros'::text
         END AS situacao_contrato
  FROM ordemservico ord
       JOIN contratos ct ON ct.contrato = ord.codigocontrato AND ct.codempresa = ord.codempresa AND ct.cidade = ord.cidade
       JOIN lanceservicos l ON l.codigodoserv_lanc = ord.codservsolicitado
       JOIN cidade cid ON cid.codigodacidade = ord.cidade
       JOIN regional reg ON reg.codigo = cid . codigo_regional
       JOIN enderecos ed ON ed.codigodacidade = ord.cidade AND ed.codigodologradouro = ct.enderecoconexao
       JOIN clientes cli ON cli.codigocliente = ord.codigoassinante AND cli.cidade = ord.cidade
       JOIN equipesdevenda ev ON ev.cidade = ct.cidade AND ev.codigo = ct.equipedevenda
       JOIN vendedores v ON v.codigo = ct.vendedor AND v.cidadeondetrabalha = ct.cidade AND v.equipevenda = ct.equipedevenda
  WHERE ord.codservsolicitado = ANY (ARRAY [ 11, 3961 ])
  ORDER BY reg.descricao,
           cid.nomedacidade,
           ord.d_dataatendimento;

ALTER VIEW regrasoperacao.vis_analitico_primeira_conexao
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

-- FUNÇÃO RELATÓRIO ANALITICO PRIMEIRA CONEXÃO
CREATE OR REPLACE FUNCTION relatoriospersonalizados.func_rp_analitico_primeira_conexao (
  pdatainicial date,
  pdatafinal date,
  pcidades bigint []
)
RETURNS TABLE (
  "REGIONAL" varchar,
  "CIDADE" varchar,
  "CÓDIGO_SERVIÇO" integer,
  "SERVIÇO" varchar,
  "NUM_OS" integer,
  "DATA_ATENDIMENTO" date,
  "DATA_AGENDAMENTO" date,
  "DATA_EXECUÇÃO" date,
  "CÓDIGO_ASSINANTE" integer,
  "NOME_ASSINANTE" text,
  "CONTRATO" integer,
  "ENDEREÇO" text,
  "NÚMERO" varchar,
  "COMPLEMENTO" varchar,
  "BAIRRO" varchar,
  "CEP" varchar,
  "TECNOLOGIA" text,
  "COD_MOTIVO_CANCELAMENTO" integer,
  "VENDEDOR" varchar,
  "EQUIPE_VENDA" varchar,
  "SITUACAO_CONTRATO" integer
) AS
$body$
      BEGIN
    	Create temporary table temp_rp_analitico_primeira_conexao(
            "REGIONAL" varchar (150),
            "CIDADE" varchar (30),
            "CÓDIGO_SERVIÇO" integer,
            "SERVIÇO" varchar (40),
            "NUM_OS" integer,
            "DATA_ATENDIMENTO" date,
            "DATA_AGENDAMENTO" date,
            "DATA_EXECUÇÃO" date,
            "CÓDIGO_ASSINANTE" integer,
            "NOME_ASSINANTE" text,
            "CONTRATO" integer,
            "ENDEREÇO" text,
            "NÚMERO" varchar (10),
            "COMPLEMENTO" varchar (15),
            "BAIRRO" varchar (20),
            "CEP" varchar (9),
            "TECNOLOGIA" text,
            "COD_MOTIVO_CANCELAMENTO" integer,
            "VENDEDOR" varchar (40),
            "EQUIPE_VENDA" varchar (30),
            "SITUACAO_CONTRATO" integer
		) On commit drop;

        insert into temp_rp_analitico_primeira_conexao
         select ap.regional,
                ap.cidade,
                ap.código_serviço,
                ap.serviço,
                ap.num_os,
                ap.data_atendimento,
                ap.data_agendamento,
                ap.data_execução,
                ap.código_assinante,
                ap.nome_assinante,
                ap.contrato,
                ap.endereço,
                ap.número,
                ap.complemento,
                ap.bairro,
                ap.cep,
                ap.tecnologia,
                ap.cod_motivo_cancelamento,
                ap.vendedor,
                ap.equipe_venda,
                ap.situacao_contrato
            from regrasoperacao.vis_analitico_primeira_conexao ap
        where hg.data_cadastro BETWEEN pdatainicial and pdatafinal and 
              ap.cidade = any(pcidades);
                             
        return query select * from temp_rp_analitico_primeira_conexao;
		                 
    end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100 ROWS 1000;

ALTER FUNCTION relatoriospersonalizados.func_rp_analitico_primeira_conexao (pdatainicial date, pdatafinal date, pcidades bigint [])
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

-> Encerramento de chamado OK
Conforme solução encontrada e demanda solucionada, estarei encerrando este ticket.

Porém, caso haja necessidade de interagir novamente, peço para que abra um novo chamado referenciando o mesmo.
Desde já agradeço o contato e ressalto que para quaisquer eventualidades, estarei à total disposição! 😀

OBS: Assim que encerrado, haverá uma pesquisa de satisfação sobre meu atendimento neste Ticket. Se puder me avaliar!! 😁

--------------------------------------------------------------------------------------------------------------------------------------------

->Encerramento depois de muito tempo sem retorno:
Olá, ! 
Tudo bem?

Informo que devido a falta de interação estarei encerrando este ticket.
Porém, caso haja necessidade de interagir novamente, peço para que abra um novo chamado!
Desde já agradecemos o contato, para quaisquer eventualidades estaremos à total disposição!

Teremos o maior prazer em lhe auxiliar! 😀

--------------------------------------------------------------------------------------------------------------------------------------------

--RELATÓRIO CONTAGEM NOTAS FISCAIS
select  u.descricao, nf.cfop, nf.serienf, nf.numnf, nf.d_dataemissao, nf.d_datacancelamento, cid.nomedacidade, 
cl.codigocliente, cl.nome, nf.totalnota, nf.baseicms, nf.valoricms, i.valorpis, i.valorcofins, nf.valorfust,
nf.valorfuntel, i.valoriss,  
	case
      when	i.codclassificacaoconv115 = 103 then 'TV'
      when i.codclassificacaoconv115 = 104 then 'INTERNET'
    end as tipo
FROM itensnf i
LEFT JOIN programacao p ON p.codcidade = i.codcidade AND p.codigodaprogramacao = i.codpacote
LEFT JOIN lanceservicos l ON l.codigodoserv_lanc = i.codlancservico 
JOIN parametros par ON 1 = 1
JOIN fatura f ON f.numerofatura = i.numfatura
JOIN docreceber d ON d.fatura = f.numerofatura
join clientes cl on cl.cidade = d.codigodacidade and  cl.codigocliente = d.cliente
JOIN nfviaunica nf ON nf.id::double precision = i.idnfconvenio
join unificadora u on u.codigo=nf.codunificadora
join cidade cid on cid.codigodacidade = p.codcidade
where nf.d_dataemissao BETWEEN '20220701' and '20220727'
group by u.descricao, nf.cfop, nf.serienf, nf.numnf, nf.d_dataemissao, nf.d_datacancelamento, cid.nomedacidade, 
cl.codigocliente, cl.nome, nf.totalnota, nf.baseicms, nf.valoricms, i.valorpis, i.valorcofins, nf.valorfust,
nf.valorfuntel, i.valoriss,  p.tipoponto, i.codclassificacaoconv115

--------------------------------------------------------------------------------------------------------------------------------------------

--FUNÇÃO RELATÓRIO CONTRATOS CANCELADOS POR PERÍODO E TIPO - ARGO TELECOM
CREATE OR REPLACE FUNCTION relatoriospersonalizados.func_rp_cancelamento_periodo_tipo_cancelamento (
  pdatainicial date,
  pdatafinal date
)
RETURNS TABLE (
  "CIDADE" varchar,
  "RAZAO_SOCIAL" varchar,
  "CÓDIGO_ASSI" integer,
  "NOME" varchar,
  "CONTRATO" integer,
  "NUM_OS" text,
  "SERV" varchar,
  "DATA_BAIXA" date,
  "MOTIVO_CANCELAMENTO" varchar
) AS
$body$
      BEGIN
    	Create temporary table temp_rp_cancelamento_periodo_tipo_cancelamento(
          "CIDADE" varchar(30),
          "RAZAO_SOCIAL" varchar(50),
          "CÓDIGO_ASSI" integer,
          "NOME" varchar (40),
          "CONTRATO" integer,
          "NUM_OS" text,
          "SERV" varchar(40),
          "DATA_BAIXA" date,
          "MOTIVO_CANCELAMENTO" varchar(50)
		) On commit drop;
       
        insert into temp_rp_cancelamento_periodo_tipo_cancelamento
        SELECT ci.nomedacidade AS cidade,
               e.razaosocial,
               cl.codigocliente AS cod,
               cl.nome,
               ct.contrato,
               'OS: '::text || ord.numos::text AS num,
               l.descricaodoserv_lanc AS serv,
               ord.d_databaixa AS data,
               m.descmotivo as motivo
        FROM ordemservico ord
           JOIN lanceservicos l ON l.codigodoserv_lanc = ord.codservsolicitado AND (l.situacaocontrato = ANY (ARRAY [ 5, 6 ]))
           JOIN contratos ct ON ct.contrato = ord.codigocontrato AND ct.cidade = ord.cidade /*AND ord.codempresa = ct.codempresa*/
           JOIN empresas e ON e.codempresa = ct.codempresa AND e.codcidade = ct.cidade
           JOIN clientes cl ON cl.codigocliente = ct.codigodocliente AND cl.cidade = ct.cidade
           JOIN cidade ci ON ci.codigodacidade = ord.cidade
           JOIN motivocancelamento m ON m.codmotivo = ct.motivocancelamento
        where ord.d_databaixa BETWEEN pdatainicial and pdatafinal;
                             
        return query select * from temp_rp_cancelamento_periodo_tipo_cancelamento;
		                 
    end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100 ROWS 1000;

ALTER FUNCTION relatoriospersonalizados.func_rp_cancelamento_periodo_tipo_cancelamento (pdatainicial date, pdatafinal date)
  OWNER TO postgres;
  
--------------------------------------------------------------------------------------------------------------------------------------------

--RELATÓRIO MATEUS CLIENTES VIP - CONEXÃO 
select distinct 
c.descricao as "Carteira", 
cli.codigocliente as "Código", 
cli.nome as "Nome", 
ct.contrato as "Contrato", 
v.descricaosituacao as "Situação",
tc.descricao,
cli.cpf_cnpj, 
cl.descricao as classificacao
from clientes cli 
join cidade cid on cid.codigodacidade=cli.cidade
join contratos ct on ct.cidade=cli.cidade and ct.codigodocliente=cli.codigocliente
join vis_situacaocontrato_descricao v on v.situacao=ct.situacao
join carteira c on c.codigo=ct.codcarteira
join tiposcontrato tc on tc.codigo=ct.tipodocontrato
left join classificacaocadastros cl on cl.codigo=ct.codigoclassificacaocadastro
where cli.vip = 1

--------------------------------------------------------------------------------------------------------------------------------------------

--PROGRAMAÇÃO POR CÓDIGO E BASE - RR
 select distinct p.nomedaprogramacao, p.codigodaprogramacao, cid.nomedacidade, r.descricao
from programacao p
join cidade cid on cid.codigodacidade = p.codcidade
join regional r on r.codigo = cid.codigo_regional
join empresas e on e.codcidade = cid.codigodacidade

--------------------------------------------------------------------------------------------------------------------------------------------

-- AJUSTE ERRO 121 ARQUIVO FISCO
--VER AS NOTAS GERADAS NO MÊS
select * from nfviaunica nf
where nf.d_dataemissao between '2022-07-01' and '2022-07-31'

--VERIFICAR NOTA QUE PULOU E AJUSTAR DATAS E REFERÊNCIA
select * from nfviaunica nf
where nf.numnf = 65436

--------------------------------------------------------------------------------------------------------------------------------------------

--VERIFICAR CIDADES SEM NOME IBGE E CÓDIGO IBGE
select t.nome, t.estado, t.codigoibge, t.nomecidadeibge from tablocal t
join public.cidade c on c.codigodacidade = t.codigo
where t.nomecidadeibge is null

--------------------------------------------------------------------------------------------------------------------------------------------

select DISTINCT car.descricao as empresa,
       c.nome,
       c.codigocliente,
       ci.nomedacidade,
       h.controle,
       h.atendente,
       h.d_datacadastro,
       h.t_horacadastro,
       h.d_datafechamento,
       h.t_horafechamento,
       gh.descricao,
       ah.descricao,
       (
         select distinct hh.loginaud
         from (
                select hisa.id,
                       max(hisa.idaud) as idauditoria
                from auditoria.aud_historicogeral hisa
                where hisa.controle = h.controle and
                      hisa.d_datafechamento is not null
                group by hisa.id
              ) as login
              join auditoria.aud_historicogeral hh on hh.idaud = login.idauditoria
       ) as usuario_fechou_historico,
       tp.descricao,
       st.descricaosituacao,
       CASE
       WHEN h.codigotiposituacao = 11 THEN 'ENCAMINHADO'::text
       WHEN h.codigotiposituacao = 21 THEN 'EM TRATAMENTO'::text       
       WHEN h.codigotiposituacao = 31 THEN 'FINALIZADO'::text
       WHEN h.codigotiposituacao = 41 THEN 'ENCAMINHADO PARA FINALIZAÇÃO'::text
       WHEN h.codigotiposituacao = 51 THEN 'REVERTIDO'::text
       WHEN h.codigotiposituacao = 61 THEN 'NÂO REVERTIDO'::text
       END AS codigo_situacao
from historicogeral h
     join clientes c on c.cidade = h.codigocidade and c.codigocliente = h.assinante
     join cidade ci on ci.codigodacidade = h.codigocidade and ci.codigodacidade = c.cidade
     left join telefones t on t.cidade = h.codigocidade and t.codigocliente = h.assinante
     join contratos con on con.cidade = h.codigocidade and con.contrato = h.codcontrato and h.codempresa = con.codempresa
     left join tipodecontato tp on tp.codigo = h.codigocontato
     join vis_situacaocontrato_descricao st on st.situacao = con.situacao
     left join public.carteira car on car.codigo = con.codcarteira
     left join tiposituacaohistorico sh on sh.codigo = h.controle
     left join grupohistorico gh on gh.codigo = h.grupoassunto
     left join assuntohistorico ah on ah.codigoassunto = h.assunto
where con.codcarteira = 31 and
      h.d_datacadastro BETWEEN '2022-05-20' and '2022-05-20'
order by c.nome

--------------------------------------------------------------------------------------------------------------------------------------------

SELECT * FROM regrasoperacao.vis_relatorio_indice_igpm rj


SELECT
       rj.codigodacidade,
       c.descricao,
       rj.nomedacidade,
       rj.codigocliente,
       rj.nome,
       rj.contrato,
       rj.situacao_contrato,
       rj.codigodaprogramacao,
       rj.nomedaprogramacao,
       rj.d_datadainstalacaocontrato,
       rj.d_dataativacaoprogramacao,
       rj.d_dataalttabelaprogramacao,
       rj.d_data,
       replace(rj.valoratualpacote, '.', ','),
       replace(rj.valorpacotereajustado, '.', ','),
       CONCAT(substring(rj.descricaoreajuste from 38 for 5),'%'),
       rj.contrato_id,
       rj.cont_prog_id,
       rj.valorpacotereajustado - valoratualpacote,
       to_char(ct.d_datadainstalacao,'MM/YY')
FROM reajustesefetivados rj
JOIN public.contratos ct ON ct.id = rj.contrato_id
join public.carteira c on c.codigo = ct.codcarteira

where rj.d_data BETWEEN '2022-05-01' and '2022-05-31'

--------------------------------------------------------------------------------------------------------------------------------------------

select distinct case 
       when ci.tiporeimpressao = 1 then 'Impresso'
       when ci.tiporeimpressao = 2 then 'Enviado por E-mail'
       when ci.tiporeimpressao = 3 then 'Arquivo Remessa Grafica'
       when ci.tiporeimpressao = 4 then 'Arquivo remessa débito '
       when ci.tiporeimpressao = 5 then 'Pré Impresso'
       when ci.tiporeimpressao = 6 then 'Remessa Bancária'
       when ci.tiporeimpressao = 7 then 'Cobrador Externo'
       when ci.tiporeimpressao = 8 then 'Recibo'
       when ci.tiporeimpressao = 9 then 'Email Boleto PDF'
       when ci.tiporeimpressao = 10 then 'Copiada linha digitável'
       when ci.tiporeimpressao = 11 then 'Enviada linha digitável por E-mail'
       when ci.tiporeimpressao = 12 then 'Copiada URL do boleto PDF'
       end as "TipoImpressão",
       dr.nomedoarquivoquebaixou,
       ci.usuario,
       cid.nomedacidade,
       dr.cliente,
       cli.nome,
       ct.id,
       dr.numerodocumento,
       dr.d_dataemissao,
       dr.d_datavencimento,
       dr.valordocumento,
       dr.d_datapagamento as pagamento,
       dr.valordesconto,
       dr.valorjuros,
       dr.valormulta,
       dr.valorpago,      
       dr.nossonumero,
       dr.bancorecebimento,
       ct.codcarteira,
       case 
       when ct.codcarteira = 11 then '7-ROSIMARA (MEGA1)' 
       when ct.codcarteira = 21 then '15-MARCELO (MEGA2)'
       when ct.codcarteira = 31 then 'IDEIA (TECNET)'
       when ct.codcarteira = 41 then 'CONEXÃO'
       when ct.codcarteira = 51 then 'DIRETA'
       when ct.codcarteira = 61 then 'OUTCENTER'
       when ct.codcarteira = 71 then 'WEBNET'
       when ct.codcarteira = 81 then 'WAYNET'
       when ct.codcarteira = 91 then 'STARWEB'
       end as "Carteira"
              
FROM docreceber dr
JOIN clientes cli ON cli.cidade = dr.codigodacidade AND cli.codigocliente = dr.cliente
JOIN controlereimpressoes ci on ci.nossonumero = dr.nossonumero
join cidade cid on cid.codigodacidade = dr.codigodacidade
LEFT JOIN contratos ct ON ct.cidade = cli.cidade and ct.codigodocliente = cli.codigocliente

--------------------------------------------------------------------------------------------------------------------------------------------

SELECT distinct cid.nomedacidade AS cidade,
         cli.codigocliente,
         cli.nome,
         cli.cpf_cnpj,
         CASE
           WHEN length(translate(cli.cpf_cnpj::text, '.-/ '::text, ''::text)) =
             11 THEN 'PF'::text
           ELSE 'PJ'::text
         END AS tipocliente,
         ct.contrato,
         ct.id AS idcontrato,
         CASE
           WHEN dr.reparcelamento = 1 THEN 'Reparcelado'::text
           WHEN dr.boletoequipamento = 1 THEN 'Equipamento'::text
           WHEN dr.situacao = 1 THEN 'Cancelado'::text
           ELSE 'Normal'::text
         END AS tipo,
         dr.numerodocumento,
         dr.nossonumerobanco,
         dr.d_datavencimento AS datavencimento,
         dr.valordocumento,
         dr.valorjuros,
         dr.valormulta,
         dr.valordesconto,
         dr.d_datapagamento AS datapagamento,
         dr.d_dataliquidacao AS dataliquidacao,
         dr.valorpago,
         CASE
           WHEN dr.nomedoarquivoquebaixou IS NOT NULL AND (dr.tipopagamento =
             ANY (ARRAY [ 1, 3 ])) THEN 'Retorno Bancário'::text
           WHEN dr.tipopagamento = 1 THEN 'Dinheiro'::text
           WHEN dr.tipopagamento = 2 THEN 'Cheque'::text
           WHEN dr.tipopagamento = 3 THEN 'Banco'::text
           WHEN dr.tipopagamento = 4 THEN 'Cartão de Débito'::text
           WHEN dr.tipopagamento = 5 THEN 'Cartão de Crédito'::text
           ELSE NULL::text
         END AS tipopagamento,
         l.descricao AS localcobranca,
         tc.descricao AS tipocontrato,
         CASE
           WHEN i.numfatura IS NOT NULL THEN 'SIM'::text
           ELSE 'NÃO'::text
         END AS temnf,
         cid.codigo_regional,
         c.codigo AS cod_unificadora,
         c.descricao AS empresa,
         case 
         	when ct.gerarcobranca = 0 then 'Acumular por Empresa'
            when ct.gerarcobranca = 1 then 'Somente do Contrato'
            when ct.gerarcobranca = 2 then 'Acumulado por Cliente'
         end as "gerar_cobranca"
  FROM docreceber dr
       JOIN cidade cid ON cid.codigodacidade = dr.codigodacidade
       JOIN clientes cli ON cli.cidade = dr.codigodacidade AND cli.codigocliente = dr.cliente
       JOIN regional r ON r.codigo = cid . codigo_regional
       JOIN localcobranca l ON l.codigo = dr.localcobranca
       JOIN movimfinanceiro m ON m.numfatura = dr.fatura
       JOIN contratos ct ON ct.cidade = m.cidade AND ct.codempresa = m.codempresa AND ct.contrato = m.contrato
       JOIN carteira c ON c.codigo = ct.codcarteira
       LEFT JOIN tiposcontrato tc ON tc.codigo = ct.tipodocontrato
       LEFT JOIN itensnf i ON i.numfatura = dr.fatura
       JOIN empresas e ON e.codcidade = ct.cidade AND e.codempresa =  ct.codempresa
       JOIN unificadora u ON u.codigo = e.codunificadora
WHERE dr.situacao = 0  and c.codigo = 61

--------------------------------------------------------------------------------------------------------------------------------------------

-- HISTÓRICO ENVIOS RÉGUA
select * from reguacobranca.historicoenviosregua a
where a.tipooperacao = 0 and to_char(a.criadoem, 'DD/MM/YYYY') BETWEEN '14/06/2022' and '14/06/2022'


select * from reguacobranca.historicoenviosregua a
where to_char(a.criadoem, 'DD/MM/YYYY') BETWEEN '14/06/2022' and '14/06/2022'


select a.id,
       a.criadoem::date as "Data Operação",
       a.criadoem::time as "Hora Operação",
       CASE WHEN a.tipooperacao = 0 THEN 'E-mail' ELSE 'SMS' END AS "Tipo_Operacao",
       CASE WHEN a.tipooperacao = 0 THEN a.email ELSE a.celular END AS "Destino",
       rg.descricao,
       a.textoerro,
       a.situacao
from reguacobranca.historicoenviosregua a
join reguacobranca.regra rg on rg.id = a.regraid
where a.criadoem::date BETWEEN '2022-06-14' and '2022-06-14'
order by a.criadoem

--------------------------------------------------------------------------------------------------------------------------------------------

select * 
from regrasoperacao.historicos x
WHERE x.cod_unificadora in (11,31,41,51,71) and x.codgrupoassunto = 61 and 
x.data_abertura BETWEEN '2022-05-01' and '2022-05-15'

--------------------------------------------------------------------------------------------------------------------------------------------

-- FUNÇÃO CLIENTES SEM EQUIPAMENTO
select * from public.func_clientes_sem_equipamento()

-- Clientes sem equipamentos
select cli.nome,
       ct.contrato,
       cid . nomedacidade,
       case
         when ct.situacao = 1 then 'Aguardando'
         when ct.situacao = 2 then 'Conectado'
         when ct.situacao = 3 then 'Pausado'
         when ct.situacao = 4 then 'Inadimplente'
         when ct.situacao = 5 then 'Cancelado'
         when ct.situacao = 6 then 'Endereço não Cabeado'
         when ct.situacao = 7 then 'Conectado/Inadimplente'
       end as "Situação"
from contratos ct
     join clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
     left join materiaisos mat on mat.codigocidade = ct.cidade and mat.codigoassinante = ct.codigodocliente
     left join cidade cid on ct.cidade = cid . codigodacidade
     left join public.ordemservico os on os.cidade = ct.cidade and os.codigocontrato = ct.contrato and os.codempresa = ct.codempresa and
       os.codservsolicitado = 1361
where mat.id is NULL and
      os.id is not null
	  
--------------------------------------------------------------------------------------------------------------------------------------------

--FUNÇÃO API ALTERA VENCIMENTO CONTRATO
CREATE OR REPLACE FUNCTION iris.contrato_alterar_vencimento (
  pparametro json
)
RETURNS json AS
$body$
declare
  rPar record;
  tRetorno text;
  iCont integer := 0;

  rParametro record;
  rDados record;
begin
  select into rPar ''::text as chavebase, 0::bigint as idcontrato, ''::text as usuario, 0::smallint as dia,
    0::smallint as tipoalteracao;

  /*
    tipoAlteracao: [1] Somente do contrato [2] Somente do assinante [3] Contrato e Assinante
  */

  rPar.chavebase := case when btrim(pParametro ->> 'chaveBase') = '' then null else cast(pParametro ->> 'chaveBase' as text) end;
  rPar.idcontrato := case when btrim(pParametro ->> 'idContrato') = '' then null else cast(pParametro ->> 'idContrato' as bigint) end;
  rPar.dia := case when btrim(pParametro ->> 'diaVencimento') = '' then null else cast(pParametro ->> 'diaVencimento' as bigint) end;
  rPar.tipoalteracao := case when btrim(pParametro ->> 'tipoAlteracao') = '' then null else cast(pParametro ->> 'tipoAlteracao' as bigint) end;
  rPar.usuario := case when btrim(pParametro ->> 'usuario') = '' then null else cast(pParametro ->> 'usuario' as text) end;

  -- Verificas os parâmetros obrigatórios
  if rPar.idcontrato is null or rPar.usuario is null or rPar.dia is null then
    raise exception 'Não foram enviados todos os parâmetros necessários, corrija!';
  elsif not(rPar.dia between 1 and 30) then
    raise exception 'O dia de vencimento está fora do intervalo permitido!';
  elsif rPar.tipoalteracao not in (1,2,3) then
    raise exception 'Tipo de alteração inválida!';
  end if;

  select into rParametro
  (select valor from iris.parametro where chave = 'chaveBase') as chavebase,
  (select valor from iris.parametro where chave = 'nomeBase') as nomebase,
  (select string_to_array(valor,';') from iris.parametro where chave = 'usuariosPermitidos') as usuariospermitidos;
  if rParametro.chavebase is null or rPar.chavebase is null or rPar.chavebase <> rParametro.chavebase then
    return '[]';
  elsif not(rPar.usuario = any(rParametro.usuariospermitidos)) or rParametro.usuariospermitidos is null then
    raise exception 'Usuário sem permissão para executar o procedimento!';
  end if;

  -- Valida os dados
  select into rDados ct.id, cli.id as idassinante from public.contratos ct
  join public.clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
  where ct.id = rPar.idcontrato;
  if rDados.id is null then
    raise exception 'Contrato não encontrado!';
  end if;

  -- Atualiza o contrato
  if rPar.tipoalteracao in (1,3) then
    update public.contratos set dtvencto = rPar.dia where id = rDados.id;
  end if;

  -- Atualiza assinante
  if rPar.tipoalteracao in (2,3) then
    update public.clientes set dtvencto = rPar.dia where id = rDados.idassinante;
  end if;

  tRetorno := '{"resposta" : "OK", "mensagem" : "Alteração executada com sucesso!"}';
  return tRetorno;
end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100;

ALTER FUNCTION iris.contrato_alterar_vencimento (pparametro json)
  OWNER TO hilton;
  
--------------------------------------------------------------------------------------------------------------------------------------------

--AJUSTE DADOS DA NF NO PDF
update docreceber set linhadigitavelcalculada = null, codigobarrascalculada = null, nossonumerocalculada = null, arquivopdf = null, 
url_pdf_terceiros = null,
nomedoarquivopdf = null
where id in (
select dr.id from docreceber dr 
join public.clientes cl on cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente
where length(cl.cpf_cnpj) > 14 and dr.d_datavencimento BETWEEN '2022-08-01' and '2022-08-31' and
 dr.d_datapagamento is null and dr.formadepagamento = 1 and dr.situacao = 0
) 

--------------------------------------------------------------------------------------------------------------------------------------------

-- FUNÇÃO RELATÓRIO DICI MEGABIT
CREATE OR REPLACE FUNCTION relatoriospersonalizados.func_rp_dici_internet (
  pdatadici date
)
RETURNS TABLE (
"CNPJ" text,
"ANO" integer,
"MES" integer,
"COD_IBGE" text,
"TIPO_CLIENTE" text,
"TIPO_ATENDIMENTO" text,
"TIPO_MEIO" text,
"TIPO_PRODUTO" text,
"TIPO_TECNOLOGIA" text,
"VELOCIDADE" numeric,
"QT_ACESSOS" integer
) AS
$body$
      BEGIN
    	Create temporary table temp_rp_dici_internet(
            "CNPJ" text,
            "ANO" integer,
            "MES" integer,
            "COD_IBGE" text,
            "TIPO_CLIENTE" text,
            "TIPO_ATENDIMENTO" text,
            "TIPO_MEIO" text,
            "TIPO_PRODUTO" text,
            "TIPO_TECNOLOGIA" text,
            "VELOCIDADE" numeric,
            "QT_ACESSOS" integer

		) On commit drop;

        insert into temp_rp_dici_internet
          select '24122280000179' as "CNPJ",
                 aa.anocoleta as "ANO",
                 aa.mescoleta as "MES",
                 aa.municipioibge as "COD_IBGE",
                 aa.tipocliente as "TIPO_CLIENTE",
                 aa.tipoatendimento as "TIPO_ATENDIMENTO",
                 aa.tipomeioacesso as "TIPO_MEIO",
                 aa.tipoproduto as "TIPO_PRODUTO",
                 aa.tecnologia as "TIPO_TECNOLOGIA",
                 aa.velocidadecontratada as "VELOCIDADE",
                 sum(aa.quantidade) as "QT_ACESSOS"
          from public.funcao_dice_anatel_v3(null::int, pdatadici, 1::smallint) aa
          group by empresacoleta,anocoleta,mescoleta,municipioibge,tipocliente,tipomeioacesso,tecnologia,quantidade,tipoatendimento,
          tipoproduto,velocidadecontratada;
                             
        return query select * from temp_rp_dici_internet;
		                 
    end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100 ROWS 1000;

ALTER FUNCTION relatoriospersonalizados.func_rp_dici_internet (pdatadici date)
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

--FUNÇÃO RELATÓRIOS ASSINANTES ANIVERSÁRIOS TCM MOSSORÓ

CREATE OR REPLACE FUNCTION relatoriospersonalizados.func_rp_assinantes_aniversarios (
  pdia integer,
  pmes text
)
RETURNS TABLE (
  "Codigo_Cidade" integer,
  "Cidade" varchar,
  "Código do Cliente" integer,
  "Nome" varchar,
  "Data de Nascimento" date,
  "Dia" integer,
  "Mês" text,
  "Telefone" varchar,
  "Situação" text
) AS
$body$
 BEGIN
 Create temporary table temp_rp_assinantes_aniversarios(
       "Codigo_Cidade" INTEGER,
       "Cidade" VARCHAR (30),
       "Código do Cliente" INTEGER,
       "Nome" VARCHAR (40),
       "Data de Nascimento" date,
       "Dia" INTEGER,
       "Mês" text,
       "Telefone" VARCHAR (500),
       "Situação" text
) On commit drop;
 
 insert into temp_rp_assinantes_aniversarios
 select aa.codigodacidade as Codigo_Cidade, 
       aa.cidade as Cidade,
       aa.cdigo_cliente as Cliente,
       aa.nome as Nome,
       aa.data_nascimento as Data_de_Nascimento,
       aa.dia as Dia,
       aa.mes as Mês,
       array_agg(split_part(aa.telefone, '[', 1)) as Telefone,
       array_agg(split_part(aa.situacao, '[', 1)) as Situcação
 from regrasoperacao.vis_assinantes_aniversarios aa
 where aa.dia = pdia and aa.mes = pmes
 GROUP BY 1,2,3,4,5,6,7;
 
 return query select * from temp_rp_assinantes_aniversarios;
 
 end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100 ROWS 1000;

ALTER FUNCTION relatoriospersonalizados.func_rp_assinantes_aniversarios (pdia integer, pmes text)
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

--FUNÇÃO RELATÓRIO PRODUTOR POR CONTRATO CONEXÃO
CREATE OR REPLACE FUNCTION relatoriospersonalizados.func_rp_produtos_por_contrato (
)
RETURNS TABLE (
"COD_CIDADE" INTEGER,
"CIDADE" varchar,
"COD_CLIENTE" integer,
"NOME_CLIENTE" varchar,
"CONTRATO_CLIENTE" integer,
"STATUS_CONTRATUAL" text,
"CARTEIRA" varchar,
"COD_PROG" integer,
"PACOTE" varchar,
"VALOR" numeric,
"PRODUTO_CONTRATO" varchar
) AS
$body$
 BEGIN
 Create temporary table temp_rp_produtos_por_contrato(
"COD_CIDADE" INTEGER,
"CIDADE" varchar (30),
"COD_CLIENTE" integer,
"NOME_CLIENTE" varchar (40),
"CONTRATO_CLIENTE" integer,
"STATUS_CONTRATUAL" text,
"CARTEIRA" varchar (50),
"COD_PROG" integer,
"PACOTE" varchar (30),
"VALOR" numeric (15,2),
"PRODUTO_CONTRATO" varchar (60)
) On commit drop;
 
 insert into temp_rp_produtos_por_contrato
 select distinct
	   cid.codigodacidade,
       cid.nomedacidade,
       ct.codigodocliente,
       cli.nome,
       ct.contrato,
       CASE
        WHEN ct.situacao = 1 THEN 'AGUAR. CONEXÃO'::TEXT
        WHEN ct.situacao = 2 THEN 'CONECTADO ATIVO'::TEXT
        WHEN ct.situacao = 3 THEN 'PAUSADO'::TEXT
        WHEN ct.situacao = 4 THEN 'INADIMPLENTE'::TEXT
        WHEN ct.situacao = 5 THEN 'CANCELADO'::TEXT
        WHEN ct.situacao = 6 THEN 'À CANCELAR'::TEXT
        WHEN ct.situacao = 7 THEN 'CONECTADO INADIMPLENTE'::TEXT           
       END as status_contratual,
       car.descricao,
       p.codigodaprogramacao,
       p.nomedaprogramacao,
       cp.valorpacote,
       pro.descricao
from contratos ct
join clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
join cidade cid on cid.codigodacidade = ct.cidade
join carteira car on car.codigo = ct.codcarteira
join cont_prog cp on cp.cidade = ct.cidade and cp.contrato = ct.contrato
join programacao p on p.codcidade = cp.cidade and p.codigodaprogramacao = cp.protabelaprecos
join materiaisos os on os.numerocontrato = ct.contrato and os.codigocidade = ct.cidade 
join produtos pro on pro.codigo = os.codigomaterial
where pro.controlado = 1
order by cid.nomedacidade, ct.codigodocliente, cli.nome;
 
 return query select * from temp_rp_produtos_por_contrato;
 
 end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100 ROWS 1000;

ALTER FUNCTION relatoriospersonalizados.func_rp_produtos_por_contrato ()
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

--UPDATE EXCLUSÃO DATA SCPC TERA OPEN
update docreceber set d_dataspc = Null, d_dataexclusaospc = Null
from (
select dr.id, dr.d_dataspc, dr.d_dataexclusaospc
from docreceber dr
where dr.numerodocumento IN ('7234','7235','7269','7270','7271','7455','7456','7457','7522','7523','7709','7710','7711',
'7801','7801','7802','7802','7803','7803','7804','7804','8702','8703','8704','8705','8706',
'8707','8771','8772','8773','8774','8775','8776','8796','8797','8798','8799','8800','8935',
'8936','8937','8938','8939','8940','9236','9237','9238','9239','9240','9241','9242','9283',
'9284','9285','9286','9287','9337','9338','9339','9340','9341','9342','9343','9583','9584',
'9585','9586','9587','9588','9589','9599','9600','9601','9602','9603','9604','9605','9674',
'9675','9676','9677','9678','9679','9680','9723','9724','9725','9726','9727','9728','9729',
'9864','9865','9866','9867','9868','9869','9870','9871','9872','10027','10028','10029','10030',
'10031','10032','10033','10034','10051','10052','10053','10054','10055','10056','10060','10061','10062',
'10063','10064','10065','10066','10082','10082','10083','10083','10084','10084','10085','10085','10086',
'10086','10087','10087','10088','10088','10089','10089','10138','10139','10140','10175','10176','10177',
'10178','10179','10180','10434','10435','10436','10437','10438','10439','10440','10441','10468','10469',
'10470','10471','10472','10473','10481','10482','10483','10484','10485','10486','10487','10488','10489',
'10507','10508','10509','10510','10511','10512','10513','10514','10515','10516','10594','10595','10596',
'10597','10598','10599','10600','10601','10602','10603','10621','10622','10623','10624','10625','10626',
'10627','10675','10676','10677','10678','10679','10680','10681','10682','10713','11129','11130','11131',
'11132','11133','11134','11135','11136','11153','11154','11155','11156','11157','11158','11194','11195',
'11196','11197','11198','11201','11202','11203','11204','11206','11207','11208','11209','11210','11211',
'11212','11213','11214','11215','11216','11232','11233','11234','11235','11236','11237','11238','11239',
'11240','11352','11353','11354','11355','11864','11865','11866','11867','11868','11869','11870','11871',
'11872','11873','11875','11876','11877','11878','11879','11880','11881','11882','11884','11990','11991',
'11992','11993','11994','11995','11996','12012','12013','12014','12015','12016','12017','12018','12019',
'12020','12257','12258','12304','12305','12306','12307','12308','12309','12310','12311','12312','12313',
'12482','12483','12484','12485','12486','12487','12488','12489','12490','12491','12492','12493','12494',
'12495','12496','12497','12498','12499','12500','12501','12502','12503','12504','12505','12506','12519',
'12520','12521','12532','12533','12662','12777','12778','12779','12780','12781','12782','12783','12784',
'12785','12786','12787','12788','13473','13474','13475','13476','13477','13478','13479','13480','13481',
'13482','13483','13484','13485','13486','13487','13488','13489','13490','13491','13492','13493','13494',
'13495','13496','13497','13499','13500','13501','13502','13503','13504','13505','13506','13507','13508',
'13509','13510','13516','13517','13518','13519','13520','13521','13522','13545','13545','13546','13546',
'13547','13547','13548','13548','13549','13549','13550','13550','13551','13551','13552','13552','13553',
'13553','13554','13554','13555','13555','13694','13695','13929','13981','13982','13983','13984','13985',
'13986','13987','13988','13989','13990','13991','13992','13993','14003','14004','14005','14018','14019',
'14204','14205','14206','14207','14208','14209','14210','14211','14212','14213','14848','14896','15034',
'15062','15063','15064','15065','15066','15067','15068','15069','15146','15146','15147','15147','15148',
'15148','15149','15149','15150','15150','15151','15151','15152','15152','15153','15153','15154','15154',
'15155','15155','15156','15156','15332','15333','15334','15335','15336','15337','15526','15527','15563',
'15848','15915','15916','15917','15918','15919','15920','15921','15922','15923','15924','15926','15927',
'15928','15929','15930','15931','15932','15933','15935','15937','15938','15939','15940','15941','15942',
'15943','15944','15945','15946','15947','15961','16031','16032','16033','16034','16035','16048','16252',
'16257','16602','16603','16745','16765','16766','16852','16854','16855','16856','17158','17320','17366',
'17367','17368','17369','17370','17371','17372','17373','17374','17405','17660','17728','18495','18510',
'18520','18542','18562','18622','18623','1002791-1/1-21','1002861-1/1-21','1003231-1/1-21','1010561-1/1-21','1011121-1/1-21','1011131-1/1-21','1013601-1/1-21','1013631-1/1-21',
'1013701-1/1-21','1014971-1/1-21','1015931-1/1-21','1016411-1/1-21','1016761-1/1-21','1017951-1/1-21','1020081-1/1-21','1020091-1/1-21','1020101-1/1-21','1020111-1/1-21','1020121-1/1-21','1020131-1/1-21','1020141-1/1-21',
'1020201-1/1-21','1020211-1/1-21','1020221-1/1-21','1020231-1/1-21','1020241-1/1-21','1020251-1/1-21','1020291-1/1-21','1020301-1/1-21','1020311-1/1-21','1020321-1/1-21','1020331-1/1-21','1020341-1/1-21','1020511-1/1-21',
'1020521-1/1-21','1020531-1/1-21','1020541-1/1-21','1020551-1/1-21','1020561-1/1-21','1020571-1/1-21','1020581-1/1-21','1020661-1/1-21','1020671-1/1-21','1020681-1/1-21','1020691-1/1-21','1020701-1/1-21','1020711-1/1-21',
'1212101-1/1-21','1212111-1/1-21','1212121-1/1-21','1212131-1/1-21','1212141-1/1-21','1212151-1/1-21','1212161-1/1-21','1212241-1/1-21','1212251-1/1-21','1212261-1/1-21','1212271-1/1-21','1212281-1/1-21','1212291-1/1-21',
'1212301-1/1-21','1212311-1/1-21','1212321-1/1-21','1212331-1/1-21','1212341-1/1-21','1212351-1/1-21','1212361-1/1-21','1212371-1/1-21','1212381-1/1-21','1212391-1/1-21','1212401-1/1-21','1212411-1/1-21','1212421-1/1-21',
'1212431-1/1-21','1212441-1/1-21','1212451-1/1-21','1212461-1/1-21','1212471-1/1-21','1212481-1/1-21','1212491-1/1-21','1212501-1/1-21','1212771-1/1-21','1212781-1/1-21','1212791-1/1-21','1212801-1/1-21','1212811-1/1-21',
'1213261-1/1-21','1217641-1/1-21','1218131-1/1-21','1218671-1/1-21','1218911-1/1-21','1221251-1/1-21','1221391-1/1-21','1221681-1/1-21','1221751-1/1-21','1222121-1/1-21','1225711-1/1-21','1227561-1/1-21','1230011-1/1-21',
'1230021-1/1-21','1232531-1/1-21','1232601-1/1-21','1233841-1/1-21','1234031-1/1-21','1234851-1/1-21','1235311-1/1-21','1235661-1/1-21','1236861-1/1-21','1238061-1/1-21','1239221-1/1-21','1239231-1/1-21','1239241-1/1-21',
'1239251-1/1-21','1239261-1/1-21','1239271-1/1-21','1239771-1/1-21','1239781-1/1-21','1239791-1/1-21','1239801-1/1-21','1239811-1/1-21','1239821-1/1-21','1240011-1/1-21','1240031-1/1-21','1240041-1/1-21','1240051-1/1-21',
'1240061-1/1-21','1240071-1/1-21','1240081-1/1-21','1240111-1/1-21','1240121-1/1-21','1240131-1/1-21','1240141-1/1-21','1240151-1/1-21','1240161-1/1-21','1240171-1/1-21','1240181-1/1-21','1240191-1/1-21','1240201-1/1-21',
'1240211-1/1-21','1240221-1/1-21','1240231-1/1-21','1240241-1/1-21','1241371-1/1-21','1242801-1/1-21','1243281-1/1-21','1243411-1/1-21','1244611-1/1-21','1244671-1/1-21','1245091-1/1-21','1248921-1/1-21','1249001-1/1-21',
'1250131-1/1-21','1250781-1/1-21','1251461-1/1-21','1251671-1/1-21','1252901-1/1-21','1253561-1/1-21','1257621-1/1-21','1257661-1/1-21','1259651-1/1-21','1260651-1/1-21','1261311-1/1-21','1261851-1/1-21','1263921-1/1-21',
'1264011-1/1-21','1266421-1/1-21','1266431-1/1-21','1266441-1/1-21','1266451-1/1-21','1266461-1/1-21','1266471-1/1-21','1266481-1/1-21','1266491-1/1-21','1266501-1/1-21','1266511-1/1-21','1266521-1/1-21','1266531-1/1-21',
'1266551-1/1-21','1266561-1/1-21','1266571-1/1-21','1266581-1/1-21','1266641-1/1-21','1266651-1/1-21','1266661-1/1-21','1266671-1/1-21','1266701-1/1-21','1266711-1/1-21','1266721-1/1-21','1266731-1/1-21','1266821-1/1-21',
'1266831-1/1-21','1266841-1/1-21','1266851-1/1-21','1269771-1/1-21','1269781-1/1-21','1269791-1/1-21','1269801-1/1-21','1269811-1/1-21','1269821-1/1-21','1271241-1/1-21','1272631-1/1-21','1272851-1/1-21','1273111-1/1-21',
'1273241-1/1-21','1273451-1/1-21','1274451-1/1-21','1274521-1/1-21','1274881-1/1-21','1277921-1/1-21','1278641-1/1-21','1278731-1/1-21','1280051-1/1-21','1280471-1/1-21','1280571-1/1-21','1280801-1/1-21','1282561-1/1-21',
'1282941-1/1-21','1283231-1/1-21','1285391-1/1-21','1287301-1/1-21','1288411-1/1-21','1288671-1/1-21','1289281-1/1-21','1289601-1/1-21','1290261-1/1-21','1290651-1/1-21','1290921-1/1-21','1293641-1/1-21','1298201-1/1-21',
'1298211-1/1-21','1298221-1/1-21','1298231-1/1-21','1298241-1/1-21','1298251-1/1-21','1298261-1/1-21','1298271-1/1-22','1298351-1/1-21','1298361-1/1-21','1298371-1/1-21','1298381-1/1-21','1298391-1/1-21','1298681-1/1-21',
'1298691-1/1-21','1298701-1/1-21','1298711-1/1-21','1298721-1/1-21','1298751-1/1-21','1298761-1/1-21','1298771-1/1-21','1298781-1/1-21','1298901-1/1-21','1298911-1/1-21','1298921-1/1-21','1298931-1/1-21','1298981-1/1-21',
'1298991-1/1-21','1299001-1/1-21','1299011-1/1-21','1299021-1/1-21','1299291-1/1-21','1299301-1/1-21','1299311-1/1-21','1299321-1/1-21','1299361-1/1-21','1299371-1/1-21','1299381-1/1-21','1299391-1/1-21','1299401-1/1-21',
'1299411-1/1-21','1300621-1/1-21','1301991-1/1-21','1302241-1/1-21','1302851-1/1-21','1304251-1/1-21','1307231-1/1-21','1307971-1/1-21','1308061-1/1-21','1309351-1/1-21','1309421-1/1-21','1309791-1/1-21','1309881-1/1-21',
'1310111-1/1-21','1311501-1/1-21','1312251-1/1-21','1314461-1/1-21','1314711-1/1-21','1316561-1/1-21','1317681-1/1-21','1317941-1/1-21','1318861-1/1-21','1318971-1/1-21','1319491-1/1-21','1319881-1/1-21','1320151-1/1-21',
'1320431-1/1-21','1327381-1/1-21','1327391-1/1-21','1327401-1/1-21','1327411-1/1-21','1327421-1/1-21','1327431-1/1-21','1327441-1/1-21','1327481-1/1-21','1327491-1/1-21','1327501-1/1-21','1327511-1/1-21','1327521-1/1-21',
'1327531-1/1-21','1327541-1/1-21','1327551-1/1-21','1327561-1/1-21','1327571-1/1-21','1327581-1/1-21','1327591-1/1-21','1327601-1/1-21','1327611-1/1-21','1327621-1/1-21','1327631-1/1-21','1327641-1/1-21','1327651-1/1-21',
'1327721-1/1-21','1327731-1/1-21','1327741-1/1-21','1327751-1/1-21','1327761-1/1-21','1327771-1/1-22','1327781-1/1-21','1327831-1/1-22','1327841-1/1-22','1327851-1/1-21','1327861-1/1-21','1327871-1/1-21','1327881-1/1-21',
'1327891-1/1-21','1327931-1/1-21','1327941-1/1-21','1327951-1/1-21','1327961-1/1-21','1327981-1/1-21','1327991-1/1-21','1328001-1/1-21','1328011-1/1-21','1328021-1/1-21','1328121-1/1-21','1328221-1/1-21','1328341-1/1-21',
'1328351-1/1-21','1328361-1/1-21','1328371-1/1-21','1328381-1/1-21','1328391-1/1-22','1329261-1/1-21','1329911-1/1-21','1331141-1/1-21','1331481-1/1-21','1331741-1/1-21','1331771-1/1-21','1333121-1/1-21','1333831-1/1-21',
'1336071-1/1-21','1336461-1/1-21','1336801-1/1-21','1338131-1/1-21','1338211-1/1-21','1338571-1/1-21','1338661-1/1-21','1338881-1/1-21','1339201-1/1-21','1340251-1/1-21','1340281-1/1-21','1340931-1/1-21','1341041-1/1-21',
'1342031-1/1-21','1342641-1/1-21','1342811-1/1-21','1343231-1/1-21','1343461-1/1-21','1344521-1/1-21','1346361-1/1-21','1346601-1/1-21','1347491-1/1-21','1347581-1/1-21','1347841-1/1-21','1348501-1/1-21','1349041-1/1-21',
'1351191-1/1-21','1351421-1/1-21','1354861-1/1-21','1356041-1/1-21','1356271-1/1-21','1356281-1/1-21','1356291-1/1-21','1356301-1/1-21','1356311-1/1-21','1356321-1/1-22','1356361-1/1-21','1356371-1/1-21','1356381-1/1-21',
'1356391-1/1-21','1356401-1/1-22','1356461-1/1-21','1356511-1/1-21','1356521-1/1-21','1356531-1/1-21','1356541-1/1-21','1356551-1/1-21','1356561-1/1-22','1356581-1/1-21','1356611-1/1-21','1356701-1/1-21','1359581-1/1-21',
'1361031-1/1-21','1364831-1/1-21','1365771-1/1-21','1366711-1/1-21','1367511-1/1-21','1369551-1/1-21','1372391-1/1-21','1372441-1/1-21','1372611-1/1-21','1373111-1/1-21','1373381-1/1-21','1373421-1/1-21','1373921-1/1-21',
'1375251-1/1-21','1375671-1/1-21','1376821-1/1-21','1377221-1/1-21','1377511-1/1-21','1377991-1/1-21','1378011-1/1-21','1379051-1/1-21','1379071-1/1-21','1379131-1/1-21','1379141-1/1-21','1379221-1/1-21','1379281-1/1-21',
'1379291-1/1-21','1379401-1/1-21','1379581-1/1-21','1379591-1/1-21','1379601-1/1-21','1379921-1/1-21','1380201-1/1-21','1380301-1/1-21','1380611-1/1-21','1380661-1/1-21','1380671-1/1-21','1381631-1/1-21','1382351-1/1-21',
'1382971-1/1-21','1384191-1/1-21','1384201-1/1-21','1384211-1/1-21','1384221-1/1-21','1384231-1/1-22','1384241-1/1-22','1384251-1/1-21','1384261-1/1-21','1384271-1/1-21','1384281-1/1-21','1384291-1/1-22','1384301-1/1-22',
'1384481-1/1-21','1384491-1/1-21','1384501-1/1-21','1384511-1/1-22','1384521-1/1-22','1384531-1/1-22','1384581-1/1-21','1384641-1/1-21','1384721-1/1-21','1384731-1/1-21','1384741-1/1-22','1384751-1/1-22','1384761-1/1-21',
'1387491-1/1-21','1392641-1/1-21','1393591-1/1-21','1394501-1/1-21','1398531-1/1-21','1399981-1/1-21','1400031-1/1-21','1400191-1/1-21','1400941-1/1-21','1400971-1/1-21','1401471-1/1-21','1402811-1/1-21','1403211-1/1-21',
'1403321-1/1-21','1403961-1/1-21','1404341-1/1-21','1404731-1/1-21','1405021-1/1-21','1405661-1/1-21','1405911-1/1-21','1406511-1/1-21','1406531-1/1-21','1406661-1/1-21','1406721-1/1-21','1406731-1/1-21','1406841-1/1-21',
'1407021-1/1-21','1407031-1/1-21','1407041-1/1-21','1407341-1/1-21','1407681-1/1-21','1407761-1/1-21','1407991-1/1-21','1408181-1/1-21','1408931-1/1-21','1409781-1/1-21','1409961-1/1-21','1410671-1/1-21','1410731-1/1-21',
'1411911-1/1-21','1412231-1/1-21','1412241-1/1-21','1412251-1/1-22','1412471-1/1-21','1416661-1/1-21','1421291-1/1-21','1422141-1/1-21','1423221-1/1-21','1424291-1/1-21','1426111-1/1-21','1427551-1/1-21','1427751-1/1-21',
'1428431-1/1-21','1428461-1/1-21','1428941-1/1-21','1430251-1/1-21','1431361-1/1-21','1431711-1/1-21','1432091-1/1-21','1432381-1/1-21','1432981-1/1-21','1433231-1/1-21','1433781-1/1-21','1433801-1/1-21','1433931-1/1-21',
'1433991-1/1-21','1434001-1/1-21','1434081-1/1-21','1434111-1/1-21','1434281-1/1-21','1434291-1/1-21','1434371-1/1-21','1434591-1/1-21','1434921-1/1-21','1434991-1/1-21','1435211-1/1-21','1435401-1/1-21','1435501-1/1-21',
'1436131-1/1-21','1437331-1/1-21','1437501-1/1-21','1438351-1/1-21','1438411-1/1-21','1439691-1/1-21','1439701-1/1-21','1439711-1/1-22','1450461-1/1-21','1451511-1/1-21','1454601-1/1-21','1455431-1/1-21','1455451-1/1-21',
'1455941-1/1-21','1458301-1/1-21','1458991-1/1-21','1459271-1/1-21','1460101-1/1-21','1460591-1/1-21','1460841-1/1-21','1461011-1/1-21','1461081-1/1-21','1461531-1/1-21','1461961-1/1-21','1462041-1/1-21','1462631-1/1-21',
'1463191-1/1-21','1464121-1/1-21','1464241-1/1-21','1465341-1/1-21','1465401-1/1-21','1466451-1/1-21','1466871-1/1-21','1477131-1/1-21','1478191-1/1-21','1481271-1/1-21','1481941-1/1-21','1482111-1/1-21','1482131-1/1-21',
'1482211-1/1-21','1482621-1/1-21','1484971-1/1-21','1485661-1/1-21','1485921-1/1-21','1486691-1/1-21','1487061-1/1-21','1487151-1/1-21','1487371-1/1-21','1487531-1/1-21','1487601-1/1-21','1488051-1/1-21','1488391-1/1-21',
'1488501-1/1-21','1489491-1/1-21','1490661-1/1-21','1490891-1/1-21','1491001-1/1-21','1491131-1/1-21','1492131-1/1-21','1492191-1/1-21','1493221-1/1-21','1493521-1/1-22','1493531-1/1-22','1493541-1/1-22','1493971-1/1-22',
'1493981-1/1-22','1493991-1/1-22','1494281-1/1-22','1494371-1/1-22','1494501-1/1-22','1495501-1/1-22','1495561-1/1-22','1496591-1/1-22','1497461-1/1-22','1502151-1/1-22','1504401-1/1-22','1506711-1/1-22','1507751-1/1-22',
'1510741-1/1-22','1511411-1/1-22','1511581-1/1-22','1511601-1/1-22','1512061-1/1-22','1514301-1/1-22','1514941-1/1-22','1515631-1/1-22','1515891-1/1-22','1516271-1/1-22','1516371-1/1-22','1516591-1/1-22','1516721-1/1-22',
'1517491-1/1-22','1517591-1/1-22','1520151-1/1-22','1520171-1/1-22','1520191-1/1-22','1521931-1/1-22','1526581-1/1-22','1528781-1/1-22','1531061-1/1-22','1532111-1/1-22','1535031-1/1-22','1535641-1/1-22','1535811-1/1-22',
'1535831-1/1-22','1536281-1/1-22','1538471-1/1-22','1539081-1/1-22','1539741-1/1-22','1539991-1/1-22','1540321-1/1-22','1540401-1/1-22','1540731-1/1-22','1541481-1/1-22','1541581-1/1-22','1544451-1/1-22','1544481-1/1-22',
'1544751-1/1-22','1544841-1/1-22','1544961-1/1-22','1546061-1/1-22','605151-1/1-20','605161-1/1-20','605241-1/1-20','605251-1/1-20','605261-1/1-20','605471-1/1-20','605481-1/1-20','605491-1/1-20','629261-1/1-20',
'629271-1/1-20','629281-1/1-20','631671-1/1-19','633151-1/1-19','636091-1/1-19','638381-1/1-19','640261-1/1-19','648281-1/1-19','650901-1/1-19','652941-1/1-20','652951-1/1-20','652961-1/1-20','654601-1/1-20',
'654611-1/1-20','654621-1/1-20','654631-1/1-20','655171-1/1-20','655181-1/1-20','655191-1/1-20','655221-1/1-20','655231-1/1-20','655241-1/1-20','655331-1/1-20','655341-1/1-20','655351-1/1-20','655361-1/1-20',
'657581-1/1-20','659071-1/1-20','662021-1/1-20','664331-1/1-20','666231-1/1-20','671141-1/1-20','674201-1/1-20','675061-1/1-20','676841-1/1-20','676901-1/1-20','678531-1/1-20','679041-1/1-20','679051-1/1-20',
'679061-1/1-20','679071-1/1-20','679091-1/1-20','679101-1/1-20','679111-1/1-20','679121-1/1-20','679251-1/1-20','679261-1/1-20','679271-1/1-20','681551-1/1-20','683081-1/1-20','686091-1/1-20','688451-1/1-20',
'689821-1/1-20','689941-1/1-20','690421-1/1-20','694751-1/1-20','695431-1/1-20','697111-1/1-20','698491-1/1-20','699321-1/1-20','700081-1/1-20','701171-1/1-20','701221-1/1-20','702831-1/1-20','703631-1/1-20',
'703641-1/1-20','703651-1/1-20','703661-1/1-20','703841-1/1-20','703851-1/1-20','703861-1/1-20','706161-1/1-20','707651-1/1-20','709591-1/1-20','710741-1/1-20','714611-1/1-20','715091-1/1-20','715101-1/1-20',
'719451-1/1-20','720131-1/1-20','721861-1/1-20','723211-1/1-20','724061-1/1-20','724861-1/1-20','725061-1/1-20','725951-1/1-20','726001-1/1-20','727641-1/1-20','728491-1/1-20','728501-1/1-20','728511-1/1-20',
'728521-1/1-20','728531-1/1-20','728541-1/1-20','728551-1/1-20','731051-1/1-20','734491-1/1-20','739411-1/1-20','739521-1/1-20','740001-1/1-20','740011-1/1-20','744321-1/1-20','744991-1/1-20','746721-1/1-20',
'748091-1/1-20','748941-1/1-20','749761-1/1-20','749971-1/1-20','750891-1/1-20','750941-1/1-20','752881-1/1-20','752991-1/1-20','753001-1/1-20','753011-1/1-20','753021-1/1-20','753031-1/1-20','753041-1/1-20',
'753051-1/1-20','753061-1/1-20','753141-1/1-20','753151-1/1-20','753161-1/1-20','753171-1/1-20','753181-1/1-20','753191-1/1-20','753201-1/1-20','753211-1/1-20','753221-1/1-20','753231-1/1-20','753241-1/1-20',
'753251-1/1-20','753261-1/1-20','753271-1/1-20','753281-1/1-20','753291-1/1-20','753301-1/1-20','753311-1/1-20','753321-1/1-20','753331-1/1-20','753691-1/1-20','754021-1/1-20','754261-1/1-20','754481-1/1-20',
'755051-1/1-20','758821-1/1-20','760581-1/1-20','765561-1/1-20','766101-1/1-20','769461-1/1-20','770391-1/1-20','771091-1/1-20','771821-1/1-20','772841-1/1-20','772881-1/1-20','775101-1/1-20','775501-1/1-20',
'776161-1/1-20','778781-1/1-20','779061-1/1-20','779241-1/1-20','779251-1/1-20','779331-1/1-20','779341-1/1-20','779351-1/1-20','779361-1/1-20','780191-1/1-20','780491-1/1-20','780501-1/1-20','780541-1/1-20',
'780551-1/1-20','780561-1/1-20','780571-1/1-20','780661-1/1-20','780671-1/1-20','780681-1/1-20','780691-1/1-20','780701-1/1-20','780871-1/1-20','780881-1/1-20','780891-1/1-20','780901-1/1-20','781441-1/1-20',
'787081-1/1-20','792201-1/1-20','792751-1/1-20','796221-1/1-20','797491-1/1-20','798621-1/1-20','799651-1/1-20','799691-1/1-20','801041-1/1-20','805751-1/1-20','806061-1/1-20','806241-1/1-20','806251-1/1-20',
'806261-1/1-20','806271-1/1-20','806281-1/1-20','806291-1/1-20','806471-1/1-20','806551-1/1-20','807071-1/1-20','811031-1/1-20','818561-1/1-20','822041-1/1-20','823371-1/1-20','824451-1/1-20','825571-1/1-20',
'826931-1/1-20','831681-1/1-20','832251-1/1-20','832261-1/1-20','832271-1/1-20','832281-1/1-20','832291-1/1-20','832301-1/1-20','837151-1/1-20','841821-1/1-20','847211-1/1-20','849631-1/1-20','853201-1/1-20',
'854251-1/1-20','858531-1/1-20','858541-1/1-20','858551-1/1-20','858561-1/1-20','858571-1/1-21','858581-1/1-21','858591-1/1-21','858671-1/1-20','858681-1/1-20','858691-1/1-20','858701-1/1-20','858711-1/1-21',
'858721-1/1-21','858731-1/1-21','858741-1/1-20','858751-1/1-20','858761-1/1-20','858771-1/1-20','858781-1/1-21','858791-1/1-21','858801-1/1-21','858811-1/1-21','858821-1/1-21','858891-1/1-20','858901-1/1-20',
'858911-1/1-20','858921-1/1-20','858931-1/1-21','858941-1/1-21','858951-1/1-21','859141-1/1-20','859151-1/1-20','859161-1/1-20','859171-1/1-21','859181-1/1-21','859191-1/1-20','859201-1/1-20','859211-1/1-20',
'859221-1/1-21','859231-1/1-21','859241-1/1-21','859391-1/1-20','859401-1/1-20','859411-1/1-20','859421-1/1-20','859431-1/1-21','859441-1/1-21','859451-1/1-21','862331-1/1-20','863841-1/1-20','868541-1/1-20',
'870781-1/1-20','871311-1/1-20','871581-1/1-20','873141-1/1-20','873991-1/1-20','876411-1/1-20','879301-1/1-20','881011-1/1-20','883581-1/1-20','885261-1/1-20','885271-1/1-20','885281-1/1-20','885291-1/1-21',
'885301-1/1-21','885691-1/1-20','885701-1/1-20','885711-1/1-21','885721-1/1-21','885741-1/1-20','885751-1/1-20','885761-1/1-20','885771-1/1-21','885781-1/1-21','885791-1/1-21','888631-1/1-20','890161-1/1-20',
'894941-1/1-20','897191-1/1-20','897731-1/1-20','898011-1/1-20','899451-1/1-20','899631-1/1-20','900211-1/1-20','900491-1/1-20','901421-1/1-20','905881-1/1-20','907611-1/1-20','910221-1/1-20','911851-1/1-20',
'912231-1/1-20','912241-1/1-21','912251-1/1-21','912261-1/1-21','912411-1/1-20','912421-1/1-21','912431-1/1-21','912441-1/1-21','912451-1/1-21','915441-1/1-20','921731-1/1-20','924061-1/1-20','924601-1/1-20',
'924881-1/1-20','926351-1/1-20','926521-1/1-20','927111-1/1-20','927381-1/1-20','928291-1/1-20','932671-1/1-20','935201-1/1-20','937041-1/1-20','937061-1/1-20','938891-1/1-20','938901-1/1-21','938911-1/1-21',
'938921-1/1-21','938931-1/1-20','938941-1/1-21','938951-1/1-21','938961-1/1-21','938971-1/1-21','938981-1/1-21','939071-1/1-21','939081-1/1-21','939091-1/1-21','939101-1/1-21','939111-1/1-21','939121-1/1-21',
'939131-1/1-20','939141-1/1-21','939151-1/1-21','939161-1/1-21','939251-1/1-21','939261-1/1-21','939271-1/1-21','939281-1/1-21','939291-1/1-21','939301-1/1-20','939311-1/1-21','939321-1/1-21','939331-1/1-21',
'939341-1/1-21','939351-1/1-21','939541-1/1-20','939551-1/1-21','939561-1/1-21','939581-1/1-20','939591-1/1-20','939601-1/1-20','939611-1/1-20','944691-1/1-20','945931-1/1-20','951561-1/1-20','953291-1/1-20',
'954011-1/1-20','955181-1/1-20','956121-1/1-20','959171-1/1-20','961471-1/1-20','962041-1/1-20','963891-1/1-20','965621-1/1-20','965631-1/1-21','965641-1/1-21','965651-1/1-21','965661-1/1-21','965671-1/1-21',
'965841-1/1-20','965851-1/1-21','965861-1/1-21','965871-1/1-21','965881-1/1-21','965891-1/1-21','966321-1/1-21','966331-1/1-21','966341-1/1-21','966351-1/1-21','966361-1/1-21','966371-1/1-21','966461-1/1-21',
'966471-1/1-21','966481-1/1-21','966491-1/1-21','966501-1/1-21','966511-1/1-21','971101-1/1-21','973491-1/1-21','973811-1/1-21','976341-1/1-21','977841-1/1-21','978501-1/1-21','982021-1/1-21','982641-1/1-21',
'987441-1/1-21','987931-1/1-21','989471-1/1-21','990161-1/1-21','990631-1/1-21','990911-1/1-21','991771-1/1-21','992671-1/1-21','992681-1/1-21','992691-1/1-21','992701-1/1-21','992711-1/1-21','992721-1/1-21',
'992801-1/1-21','992811-1/1-21','992821-1/1-21','992831-1/1-21','993051-1/1-21','993061-1/1-21','993071-1/1-21','993081-1/1-21','993091-1/1-21','993101-1/1-21','993111-1/1-21','993121-1/1-21','993131-1/1-21',
'993141-1/1-21','993221-1/1-21','993231-1/1-21','993241-1/1-21','993251-1/1-21','993341-1/1-21','993351-1/1-21','993361-1/1-21','993371-1/1-21','993381-1/1-21','993391-1/1-21','993481-1/1-21','993521-1/1-21',
'993551-1/1-21','993561-1/1-21','993571-1/1-21','993581-1/1-21','993591-1/1-21','993601-1/1-21','993611-1/1-21','993761-1/1-21','994031-1/1-21','994041-1/1-21','994051-1/1-21','994061-1/1-21','994071-1/1-21',
'994081-1/1-21','994391-1/1-21','999251-1/1-21'
)) --(APENAS PARA ESSE ID DE BOLETO) 
sql
where sql.id = docreceber.id

--------------------------------------------------------------------------------------------------------------------------------------------

-- VIEW RELATÓRIO CLIENTES PLANOS PRODUTOS CONEXÃO
CREATE VIEW regrasoperacao.vis_clientes_planos_produtos (
    carteira,
    empresa,
    cod_cidade,
    nome_cidade,
    codigo_cliente,
    nome_assinante,
    idcontrato,
    tipocontrato,
    data_instalacao,
    data_vencimento,
    situacao_contrato,
    nome_pacote,
    tipo_ponto,
    valor_pacote,
    valor_pacaote_desconto,
    nome_plano,
    aditivo,
    data_posicao,
    codigo_carteira,
    data_ativacao_pacote,
    cod_programacao)
AS
SELECT DISTINCT car.descricao AS carteira,
    emp.razaosocial AS empresa,
    c.codcidade AS cod_cidade,
    c.nomecidade AS nome_cidade,
    c.codassinante AS codigo_cliente,
    c.nomeassinante AS nome_assinante,
    c.idcontrato,
    c.tipocontrato,
    c.datainstalacao AS data_instalacao,
    ct.dtvencto AS data_vencimento,
    c.descricaosituacaocontrato AS situacao_contrato,
    p.nomepacote AS nome_pacote,
        CASE
            WHEN pr.tipoponto = 2 THEN 'Ponto Adicional'::text
            ELSE 'Ponto Principal'::text
        END AS tipo_ponto,
    p.valorpacote AS valor_pacote,
    func_calculavaloraditivos_v2(ct.cidade, ct.codempresa, ct.contrato, pr.tipoponto::integer, pr.tipoprogramacao::integer, p.valorpacote,
        to_char(CURRENT_DATE::timestamp with time zone, 'YYYY-MM-01'::text)::date, (to_char(CURRENT_DATE::timestamp with time zone, 'YYYY-MM-01'::text)::date + '1 mon'::interval)::date - 1, pr.codigodaprogramacao) AS valor_pacaote_desconto,
    pra.nomedaprogramacao AS nome_plano,
    array_to_string(ARRAY(
    SELECT a.descricao
    FROM aditivoscontratos ac
             LEFT JOIN aditivos a ON a.codaditivo = ac.codaditivo
    WHERE ac.codcidade = ct.cidade AND ac.codempresa = ct.codempresa AND ac.numcontrato = ct.contrato AND (to_char(CURRENT_DATE::timestamp with time
        zone, 'YYYY-MM-01'::text)::date >= ac.d_datainicio AND to_char(CURRENT_DATE::timestamp with time zone, 'YYYY-MM-01'::text)::date <= ac.d_datafim OR to_char(CURRENT_DATE::timestamp with time zone, 'YYYY-MM-01'::text)::date >= ac.d_datainicio AND to_char(CURRENT_DATE::timestamp with time zone, 'YYYY-MM-01'::text)::date <= ac.d_datafim OR to_char(CURRENT_DATE::timestamp with time zone, 'YYYY-MM-01'::text)::date < ac.d_datainicio AND to_char(CURRENT_DATE::timestamp with time zone, 'YYYY-MM-01'::text)::date > ac.d_datafim)
    ), ','::text) AS aditivo,
    c.dataposicao AS data_posicao,
    car.codigo AS codigo_carteira,
    cp.d_dataativacao AS data_ativacao_pacote,
    pr.codigodaprogramacao AS cod_programacao
FROM gerencial.contratosdiarios c
JOIN gerencial.pacotesdiarios p ON p.idcontrato = c.idcontrato AND p.dataposicao = c.dataposicao
JOIN contratos ct ON ct.id = c.idcontrato
JOIN empresas emp ON emp.codcidade = ct.cidade AND emp.codempresa = ct.codempresa
JOIN carteira car ON car.codigo = ct.codcarteira
JOIN programacao pr ON pr.id = p.idpacote
LEFT JOIN pacotesagregados pa ON pa.codcidade = pr.codcidade AND pa.codpacotepai = pr.codigodaprogramacao
LEFT JOIN programacao pra ON pra.codcidade = pa.codcidade AND pra.codigodaprogramacao = pa.codpacoteagregado
LEFT JOIN cont_prog cp ON cp.id = p.idcontprog;

ALTER VIEW regrasoperacao.vis_clientes_planos_produtos
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

--FUNÇÃO RELATÓRIO CLIENTES PLANOS PRODUTOS CONEXÃO
CREATE OR REPLACE FUNCTION relatoriospersonalizados.func_rp_clientes_planos_produtos (
pdatainicial date,
pdatafinal date
)
RETURNS TABLE (
"CARTEIRA" varchar,
"EMPRESA" varchar,
"COD_CIDADE" integer,
"NOME_CIDADE" varchar,
"CODIGO_CLIENTE" integer,
"NOME_ASSINANTE" varchar,
"IDCONTRATO" integer,
"TIPOCONTRATO" varchar,
"DATA_INSTALACAO" date,
"DATA_VENCIMENTO" integer,
"SITUACAO_CONTRATO" varchar,
"COD_PROGRAMACAO" integer,
"NOME_PACOTE" text,
"TIPO_PONTO" text,
"VALOR_PACOTE" numeric,
"VALOR_PACAOTE_DESCONTO" numeric,
"NOME_PLANO" varchar,
"ADITIVO" text,
"DATA_POSICAO" date,
"DATA_ATIVACAO_PACOTE" date
) AS
$body$
      BEGIN
    	Create temporary table temp_rp_clientes_planos_produtos(
"CARTEIRA" varchar (50),
"EMPRESA" varchar (50),
"COD_CIDADE" integer,
"NOME_CIDADE" varchar (50),
"CODIGO_CLIENTE" integer,
"NOME_ASSINANTE" varchar (70),
"IDCONTRATO" integer,
"TIPOCONTRATO" varchar (40),
"DATA_INSTALACAO" date,
"DATA_VENCIMENTO" integer,
"SITUACAO_CONTRATO" varchar (30),
"COD_PROGRAMACAO" integer,
"NOME_PACOTE" text,
"TIPO_PONTO" text,
"VALOR_PACOTE" numeric (15,2),
"VALOR_PACAOTE_DESCONTO" numeric (15,2),
"NOME_PLANO" varchar (30),
"ADITIVO" text,
"DATA_POSICAO" date,
"DATA_ATIVACAO_PACOTE" date
		) On commit drop;
       
        insert into temp_rp_clientes_planos_produtos
          select carteira,
                 empresa,
                 cod_cidade,
                 nome_cidade,
                 codigo_cliente,
                 nome_assinante,
                 idcontrato,
                 tipocontrato,
                 data_instalacao,
                 data_vencimento,
                 situacao_contrato,
                 cod_programacao,
                 nome_pacote,
                 tipo_ponto,
                 valor_pacote,
                 valor_pacaote_desconto,
                 nome_plano,
                 aditivo,
                 data_posicao,
                 data_ativacao_pacote
          from regrasoperacao.vis_clientes_planos_produtos a
          where a.data_posicao BETWEEN pdatainicial and pdatafinal;
                             
        return query select * from temp_rp_clientes_planos_produtos;
		                 
    end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100 ROWS 1000;

ALTER FUNCTION relatoriospersonalizados.func_rp_clientes_planos_produtos (pdatainicial date, pdatafinal date)
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

--VIEW RELATÓRIO CLIENTES POR CTO - CONEXÃO
CREATE VIEW regrasoperacao.vis_cto_geofocus_v2 (
    localizacao,
    descricao,
    id_caixa,
    caixa,
    porta,
    contrato_id,
    carteira,
    tipo_pessoa,
    nome,
    endereco_cnx,
    bairro_cnx,
    pct,
    equip_contrato,
    contrato,
    nomedacidade,
    situacao_contrato,
    data_instalacao)
AS
SELECT cid.nomedacidade AS localizacao,
    p.nomedoposte AS descricao,
    p.id AS id_caixa,
    p.nomedoposte AS caixa,
    ti.descricao AS porta,
    ct.id AS contrato_id,
    c.descricao AS carteira,
        CASE
            WHEN length(translate(cli.cpf_cnpj::text, '-. ;/\|%$#'::text, ''::text)) > 13 THEN 'PJ'::text
            WHEN length(translate(cli.cpf_cnpj::text, '-. ;/\|%$#'::text, ''::text)) = 11 THEN 'PF'::text
            ELSE ''::text
        END AS tipo_pessoa,
    cli.nome,
    (((e.tipodologradouro::text || ' '::text) || e.nomelogradouro::text) || ', Nº '::text) || ct.numeroconexao::text AS endereco_cnx,
    ct.bairroconexao AS bairro_cnx,
    func_retornapacotesdocontrato(ct.cidade, ct.codempresa, ct.contrato) AS pct,
    array_to_string(ARRAY(
    SELECT prod.descricao
    FROM materiaisos mat
             JOIN produtos prod ON prod.codigo = mat.codigomaterial
    WHERE mat.codigocidade = ct.cidade AND mat.codempresa = ct.codempresa AND mat.numerocontrato = ct.contrato AND mat.conversoroudecodificador IS
        NOT NULL AND mat.conversoroudecodificador::text <> ''::text AND mat.d_dataretirada IS NULL
    ), ' || '::text) AS equip_contrato,
    ct.contrato,
    cid.nomedacidade,
    v.descricaosituacao AS situacao_contrato,
    ct.d_datadainstalacao AS data_instalacao
FROM contratos ct
     JOIN postes p ON (p.id + '5000000000'::bigint) = ct.idgeofocus
     LEFT JOIN tags_itens ti ON ti.id = ct.idportacto
     JOIN cidade cid ON cid.codigodacidade = ct.cidade
     JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
     JOIN enderecos e ON e.codigodologradouro = ct.enderecoconexao AND e.codigodacidade = ct.cidade
     JOIN vis_situacaocontrato_descricao v ON v.situacao = ct.situacao
     JOIN carteira c ON c.codigo = ct.codcarteira
WHERE ct.idgeofocus >= '5000000000'::bigint
UNION
SELECT cid.nomedacidade AS localizacao,
    h.descricaoctogeofocus AS descricao,
    h.idctogeofocus AS id_caixa,
    h.descricaoctogeofocus AS caixa,
    h.portactogeofocus::text AS porta,
    ct.id AS contrato_id,
    c.descricao AS carteira,
        CASE
            WHEN length(translate(cli.cpf_cnpj::text, '-. ;/\|%$#'::text, ''::text)) > 13 THEN 'PJ'::text
            WHEN length(translate(cli.cpf_cnpj::text, '-. ;/\|%$#'::text, ''::text)) = 11 THEN 'PF'::text
            ELSE ''::text
        END AS tipo_pessoa,
    cli.nome,
    (((e.tipodologradouro::text || ' '::text) || e.nomelogradouro::text) || ', Nº '::text) || ct.numeroconexao::text AS endereco_cnx,
    ct.bairroconexao AS bairro_cnx,
    func_retornapacotesdocontrato(ct.cidade, ct.codempresa, ct.contrato) AS pct,
    array_to_string(ARRAY(
    SELECT prod.descricao
    FROM materiaisos mat
             JOIN produtos prod ON prod.codigo = mat.codigomaterial
    WHERE mat.codigocidade = ct.cidade AND mat.codempresa = ct.codempresa AND mat.numerocontrato = ct.contrato AND mat.conversoroudecodificador IS
        NOT NULL AND mat.conversoroudecodificador::text <> ''::text AND mat.d_dataretirada IS NULL
    ), ' || '::text) AS equip_contrato,
    ct.contrato,
    cid.nomedacidade,
    v.descricaosituacao AS situacao_contrato,
    ct.d_datadainstalacao AS data_instalacao
FROM contratos ct
     JOIN geofocus.visao_hps_ctos_portas h ON h.idhp = ct.idgeofocus
     JOIN cidade cid ON cid.codigodacidade = ct.cidade
     JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
     JOIN enderecos e ON e.codigodologradouro = ct.enderecoconexao AND e.codigodacidade = ct.cidade
     JOIN vis_situacaocontrato_descricao v ON v.situacao = ct.situacao
     JOIN carteira c ON c.codigo = ct.codcarteira
WHERE ct.idgeofocus < '5000000000'::bigint;

ALTER VIEW regrasoperacao.vis_cto_geofocus_v2
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

--relatório personalizado clientes por cto - conexão
select ct.id_caixa,
       ct.caixa,
       ct.localizacao,
       ct.contrato,
       ct.situacao_contrato,
       ct.data_instalacao,
       ct.nome,
       ct.endereco_cnx,
       ct.bairro_cnx,
       ct.nomedacidade,
       ct.pct,
       ct.tipo_pessoa,
       ct.equip_contrato
from regrasoperacao.vis_cto_geofocus_v2 ct

--------------------------------------------------------------------------------------------------------------------------------------------

--EXCLUSÃO CONTRATOS TESTES
select cli.nome, ct.* from contratos ct
join clientes cli on cli.cidade = ct.cidade and cli.codigocliente = ct.codigodocliente
where cli.nome ilike '%TESTE%'

--delete OS
DELETE from ordemservico WHERE codigoassinante = 140941;

--verifica se tem foto na OS
DELETE from fotosbaixaos where numos IN (
);

--delete fotoscontrato
DELETE from fotoscontrato WHERE codcontrato = 256021;

--delete historicogeral
DELETE from historicogeral where assinante = 140941;

--delete usuariodohistorico
select * from usuariosdohistorico hg
where hg.controlehistorico = 13961521 - CONTROLE historico

DELETE FROM usuariosdohistorico WHERE controlehistorico IN (
  select hg.controle from historicogeral hg
  where hg.assinante = 140941 and hg.codigocidade = 891681
)

--delete aditivos
DELETE from aditivoscontratos WHERE numcontrato = 230611;

--delete variacaodepacotes
DELETE from variacaodepacotes WHERE assinante = 140941

--delete boletos
DELETE from docreceber WHERE cliente = 140941;

--delete vendasvendedores
delete from vendasvendedores where assinante = 140941;

--delete controleimpressoes
DELETE from controlereimpressoes WHERE nossonumero IN (
SELECT dr.nossonumero from docreceber dr
where dr.cliente = 140941
);

--delete campanhadocreceber
DELETE from campanhadocreceber WHERE nossonumero IN (
SELECT dr.nossonumero from docreceber dr
where dr.cliente = 140941
);

--delete movimfinanceiro
DELETE from movimfinanceiro WHERE contrato = 230611;

--delete nfviaunica
DELETE from nfviaunica WHERE codassinante = 140941;

--desabilita trigger
alter table cont_prog disable trigger tri_cont_prog_sva_hero_ativacao;
alter table cont_prog disable trigger tri_cont_prog_sva_hero_manutencao;
alter table cont_prog disable trigger tri_cont_prog_sva_hero_retirada;
--deleta
delete ....;
--habilita trigger novamente
alter table cont_prog enable trigger tri_cont_prog_sva_hero_ativacao;
alter table cont_prog enable trigger tri_cont_prog_sva_hero_manutencao;
alter table cont_prog enable trigger tri_cont_prog_sva_hero_retirada;

--------------------------------------------------------------------------------------------------------------------------------------------

--CRIAR TABELA TEMPORÁRIA
CREATE TABLE temporarias.ajuste_pontoadc(
cod_cidade integer,
cod_prog integer,
nome_ponto varchar (30));

--------------------------------------------------------------------------------------------------------------------------------------------

--DELETE E UPDATE AJUSTE MASSIVO DE PONTO ADICIONAL - 29448 CONEXÃO
DELETE FROM programacaopacotesplay where id in (
  select ppp.id 
  from programacaopacotesplay ppp
  JOIN temporarias.ajuste_pontoadc a ON a.cod_cidade = ppp.codigocidadeprogramacao and a.cod_prog = ppp.codigoprogramacao
);


update programacao set nomedaprogramacao = 'PONTO ADICIONAL TV', nomeabreviado = 'PONTO ADICIONAL', tipoponto = 2, tipoprogramacao = 0, 
tipoequipamento = 4, codtipotecnologia = 11
where id in (
  select p.id
  from programacao p
  JOIN temporarias.ajuste_pontoadc a ON a.cod_cidade = p.codcidade and a.cod_prog = p.codigodaprogramacao and a.nome_ponto = p.nomedaprogramacao
);

--------------------------------------------------------------------------------------------------------------------------------------------

--FUNÇÃO API URA_LIBERA_CONFIANÇA TCM MOSSORÓ
declare
  biIdOS bigint;
  biNumOS bigint;
  biIdHistorico bigint;
  biNumHistorico bigint;
  iCont integer := 0;
  rTipos record;
  var_consulta text;
  rContrato record;
  rContrato1 record;
  rOS record;
  pParametro record;
  biEquipe bigint;
  considerarTaxaServico boolean := FALSE;
  servexecutado_taxaservico numeric(8,2) := 0;
  servexecutado_ignorarvalor integer := 1;
begin
    select into pParametro p.lc_codigoosinternet as posinternet, p.lc_tipoliberacaointernet as pacaointernet, p.lc_equipepadraointernet as pequipeinternet,
        p.lc_grupohistorico as pgrupohistorico, p.lc_assuntohistorico as passuntohistorico, p.lc_codigoostv, p.lc_periodoliberacoes, p.lc_qtdliberacoesperiodo,
        p.lc_contratos_situacao as psituacoesliberacao
        from parametros p;

   select into rContrato1 ct.situacao 
   from contratos ct 
   where ct.id = pContratoId;

 If rContrato1.situacao IN 
 	(SELECT unnest(string_to_array(replace (pParametro.psituacoesliberacao::text, ' '::text, ''::text), ','::text)::bigint [])) then
        select into rContrato ct.cidade, ct.codempresa, ct.contrato, ct.codigodocliente 
        from contratos ct where ct.id = pContratoId;

        select into rOS count(os.id) as qtdos
        from ordemservico os
        join contratos ct on ct.cidade=os.cidade and ct.codempresa=os.codempresa and ct.contrato=os.codigocontrato --and ct.situacao in (7)
        where ct.id = pContratoId and os.codservsolicitado in (pParametro.posinternet, pParametro.lc_codigoostv)and os.d_dataexecucao between CURRENT_DATE -pParametro.lc_periodoliberacoes
        and CURRENT_DATE having count(os.id) >= pParametro.lc_qtdliberacoesperiodo;
		     
        raise notice 'rOS.qtdos: %', rOS.qtdos;
           
        IF rOS.qtdos >= pParametro.lc_qtdliberacoesperiodo then
          return iCont;
        ELse

          -- Busca os contratos vinculados em boletos em aberto de acordo com os dias especificados no parâmetro para a cidade
          iCont := iCont + 1;
          
          IF apenasconsulta THEN 
          	return iCont;
          END IF;
          
          -- Marca todos os Pacotes Desativos
          --and c.tipoprogramacao in (1,6)
          var_consulta := 'select a.id, a.codigodoponto '||
                          'FROM CONT_PROG A ' ||
                          'JOIN PRODTABELAPRECO B ON A.CIDADE = B.CODCIDADE AND A.CODIGODATABELADEPRECOS = B.CODIGODATABELA AND A.PROTABELAPRECOS = B.CODIGODAPROGRAMACAO '||
                          'JOIN PROGRAMACAO C ON B.CODCIDADE = C.CODCIDADE AND B.CODIGODAPROGRAMACAO = C.CODIGODAPROGRAMACAO ' ||
                          'where a.cidade = '|| rContrato.Cidade ||' and a.codempresa = '|| rContrato.CodEmpresa ||' and a.contrato = '|| rContrato.Contrato ||' and '||
                          'a.situacao <> 1';

          for rTipos in execute var_consulta
          loop
              update public.cont_prog set marca = 1 where id = rTipos.id;
              update public.pontos set marca = 1 where id = rTipos.codigodoponto;
          end loop;
          
          -- Deve considerar a taxa do servico?
          select into considerarTaxaServico pcv.valor 
          from centralassinante.parametrochavevalor pcv
          join centralassinante.parametro p on p.id = pcv.parametroid 
          where p.ativo and pcv.chave = 'ConsiderarTaxaLiberacaoConfianca';
            
          IF considerarTaxaServico IS TRUE THEN
          	servexecutado_ignorarvalor = 0; -- nao ignorar
            select into servexecutado_taxaservico ps.valorservico FROM pesoservico ps 
            	WHERE ps.codservico = pParametro.posinternet AND ps.codcidade = rContrato.cidade;
          END IF;

          -- Pega a Sequencia do Historico
          select into biIdHistorico nextval('public.historicogeral_id_seq');

          -- Gera o Histórico para o Assinante
          insert into public.historicogeral(id, codigocidade, assinante, d_data, t_hora, descricao, grupoassunto, assunto, atendente,
          usuario, d_datacadastro, t_horacadastro, d_dataconclusao, t_horaconclusao, d_datafechamento, t_horafechamento, valorautorizado)
          values(biIdHistorico, rContrato.Cidade, rContrato.codigodocliente, current_date, current_time, 'EXECUÇÃO DE O.S. LIBERAÇÃO DE CONFIANÇA',
          pParametro.pGrupoHistorico, pParametro.pAssuntoHistorico, 'CENTRALMOBILE', 'CENTRALMOBILE', 
          current_date, current_time, current_date, current_time, current_date, current_time, servexecutado_taxaservico);

          -- Busca número do Histórico
          select into biNumHistorico h.controle from public.historicogeral h where h.id = biIdHistorico;

          -- Pega a Sequencia da O.S.
          select into biIdOS nextval('public.ordemservico_id_seq');

          --Busca o Código da Equipe
          select into biEquipe e.codigodaequipe from equipe e where e.codigocidade = rContrato.Cidade and e.codigounificadora = pParametro.pEquipeInternet limit 1;

          -- Gera a Ordem de Serviço para o Assinante
          insert into public.ordemservico(id, cidade, codempresa, codigoassinante, codigocontrato, codservsolicitado, d_dataagendamento, d_dataatendimento,
          t_horaatendimento, periodo, equipe, impresso, d_dataexecucao, equipeexecutou, t_horainicial, t_horafinal, atendente, situacao,
          observacoes, cidade_situacao, usuario, d_datacadastro, t_horacadastro, d_databaixa, t_horabaixa, numhistorico, valoros)
          values(biIdOS, rContrato.Cidade, rContrato.CodEmpresa, rContrato.codigodocliente, rContrato.Contrato, pParametro.pOsInternet, current_date, current_date, current_time, 'MANHÃ', biEquipe,
          1, current_date, biEquipe, current_time, current_time, 'CENTRALMOBILE', 1, 'OS GERADA PELA ROTINA DE LIBERAÇÃO DE CONFIANÇA',
          (rContrato.Cidade::text||'1')::integer, 'CENTRALMOBILE', current_date, current_time, null, null, biNumHistorico, servexecutado_taxaservico);
        	
          -- Busca o Número da O.S.
          select into biNumOS os.numos from public.ordemservico os where os.id = biIdOS;

          -- Inclui o Serviço Executado
          insert into public.servexecutadosos(cidade, codempresa, numos, codigoservico, valorservico, ignoravalor, usuario, d_datacadastro, t_horacadastro, parcelas, qtdeservicos)
          values(rContrato.Cidade, rContrato.CodEmpresa, biNumOS, pParametro.pOsInternet, servexecutado_taxaservico, servexecutado_ignorarvalor, 'CENTRALMOBILE', current_date, current_time, 1, 1);
          
          -- Baixa a ordem de serviço
          if pParametro.pAcaoInternet = 1 then
              update public.ordemservico set situacao = 3 where id = biIdOS;
          end if;
        ENd If;
        --raise notice 'iCont,%', iCont;
  Else
    iCont := 0;
  ENd If;
  return iCont;
end;

--------------------------------------------------------------------------------------------------------------------------------------------

-- PASSO A PASSO PARA ANALISE DE ARQUIVOS RETORNO - FABRÍCIO

/* OCORRÊNCIAS DO ARQUIVO DE RETORNO EM BOLETOS BAIXADOS */
select * from public.boletosbaixados b
where b.nomearquivo ilike '%CBR643.C3247821.05082022.013940%'


select * from public.boletosbaixados b
where b.nossonumero = '32478210008492949'

select * from public.boletosbaixados b
where b.numeroboleto ilike '%82663131%' = '32478210008492949'

/* 1-FILTRAR RETORNO IMPORTADO ATRAVES DO NOME DO ARQUIVO 

	LOCALIZAR CASOS DE BOLETOS PARA VALIDAR NO DOCRECEBER (PASSO 2)

*/

select *
from padroesbancarios.boletosretorno r
where r.nomearquivo ilike '%CBR643.C3247821.05082022.013940%'


/* 2-ANALISAR DOCRECEBER COM NOSSO NUMERO REGISTRO NO BANCO */

 -- 2.1 BUSCA NOSSONUMEROBANCO EQUALS
 select dr.localcobranca, dr.cliente, dr.d_datapagamento, dr.nomedoarquivoquebaixou, *
 from docreceber dr
 where dr.nossonumerobanco = '32478210007749912'

 -- 2.1 BUSCA NOSSONUMEROBANCO CONTAINS
 select dr.localcobranca, dr.cliente, dr.d_datapagamento, dr.nomedoarquivoquebaixou, *
 from docreceber dr
 where dr.nossonumerobanco::TEXT ILIKE '%7550943%'

/* 3-ANALSAR RELAÇÃO ENTRE TITULOS NO RETORNO E DOCERECEBER  */
SELECT r.nomearquivo, r.id as retorno_id, r.ocorrencia_codigo, dr.id as docreceber_id, r.convenio, cc.convenio,
	r.nossonumerobanco, dr.numerodocumento, dr.nomedoarquivoquebaixou, dr.d_datapagamento
from padroesbancarios.boletosretorno r
left join docreceber dr on dr.nossonumerobanco = r.nossonumerobanco::bigint
left join contascreditocidade cc on cc.codigocidade = dr.codigodacidade and cc.codigoconta = dr.codcontacredito
where r.nomearquivo ilike '%CBR643.C3247821.05082022.013940%'
and r.ocorrencia_codigo = '06'
and dr.id is null 
--and dr.id is not null 
and dr.d_datapagamento is null 
and dr.d_datacancelamento is null

/* 4-ANALSAR RELAÇÃO ENTRE TITULOS NO RETORNO, DOCERECEBER E BOLETOS BAIXADOS  */

SELECT  r.id as retorno_id, dr.id as docreceber_id, bx.id as boletosbaixados_id, r.ocorrencia_codigo,  dr.numerodocumento, dr.nomedoarquivoquebaixou, dr.d_datapagamento
from padroesbancarios.boletosretorno r
left join docreceber dr on dr.nossonumerobanco = r.nossonumerobanco::bigint
left join boletosbaixados bx on bx.nossonumero::bigint = dr.nossonumerobanco
where r.nomearquivo ilike '%CBR643.C3247821.05082022.013940%'
and r.ocorrencia_codigo = '06'
and dr.id is not null

select  bx.id as boletosbaixados_id, r.id as retorno_id, dr.id as docreceber_id, r.ocorrencia_codigo, bx.situacao, bx.numeroboleto, bx.codigoassinante
from boletosbaixados bx
left join padroesbancarios.boletosretorno r on r.nossonumerobanco = bx.nossonumero
left join docreceber dr on dr.nossonumerobanco = r.nossonumerobanco::bigint
where r.nomearquivo ilike '%CBR643.C3247821.05082022.013940%'
and r.ocorrencia_codigo = '06'

-- Auditoria de docreceber

select dr.id, dr.codigodacidade, dr.cliente, dr.numerodocumento, dr.nomedoarquivoquebaixou, dr.localcobranca, dr.codcontacredito, dr.nossonumerobanco, *
from auditoria.aud_docreceber dr
where dr.nossonumerobanco::text ilike '%32478210007851038%'
--32478210007851038

select dr.nomedoarquivoquebaixou, dr.localcobranca, dr.codcontacredito, dr.nossonumerobanco, *
from auditoria.aud_docreceber dr
where dr.id = 8492949  

--------------------------------------------------------------------------------------------------------------------------------------------

--CREATE TABLE TESTE PASCHOALOTTO JOÃOfirst_name

CREATE TABLE public.clientes(
id SERIAL PRIMARY KEY,
nome_str varchar (100) NOT NULL,
cpf_str varchar (30) NOT NULL,
email_str varchar (50) NOT NULL,
telefone_str varchar(30) NOT NULL
);

--------------------------------------------------------------------------------------------------------------------------------------------

--VER AGENDAMENTOS E EXECUÇÕES RÉGUA DE COBRANÇA
select * from reguacobranca.execucao a 
where a.inicio BETWEEN '20220817 00:00:00' and '20220817 23:59:59'

--------------------------------------------------------------------------------------------------------------------------------------------

--tabela temporário alteração de telefones TCM
CREATE TABLE temporarias.id_tel(
id SERIAL PRIMARY KEY,
telefone varchar (20)
);

update telefones set telefone = x.telefone
from (
select it.id, it.telefone from temporarias.id_tel it
) as x
where x.id = public.telefones.id;

--------------------------------------------------------------------------------------------------------------------------------------------

-- RELATÓRIO SHOWROOM - ATENDIMENTO ABERTO CONEXÃO
select * from regrasoperacao.historicos_v2 x
where x.tipo = 'Principal' and x.histfechado = 'Aberto' and x.data_abertura BETWEEN '20220801' and '20220802' and
x.grupo_atendente ilike '%SHOWROOM%'

--------------------------------------------------------------------------------------------------------------------------------------------

-- RELATÓRIO SHOWROOM - ATENDIMENTO ABERTO CONEXÃO BKO
select x.empresa,
       x.carteiraok,
       x.nomedacidade,
       x.codigocliente,
       x.nome,
       x.codcontrato,
       x.grupohistorico,
       x.assuntohistorico,
       x.protocolo,
       x.data_abertura,
       x.hora_abertura,
       x.recorrencia,
       x.usuario_abriu,
       x.grupo_atendente,
       x.data_primeiro_andamento,
       x.tipo,
       x.histfechado,
       x.datafechamento,
       x.tempo_ate_fechamento,
       x.usuario_fechou_historico,
       x.sit_hisstorico,
       x.horas_execucao,
       x.usuarioresponsavel,
       x.tempo_abertura_ate_primeiro_andamento,
       x.cod_unificadora,
       x.codgrupoassunto,
       x.carteira
from regrasoperacao.historicos_v2 x
where x.tipo = 'Principal' and x.histfechado = 'Aberto' and x.codgrupoassunto = 61

--------------------------------------------------------------------------------------------------------------------------------------------

--RELATÓRIO CSP - COMERCIAL - PROGRAMAÇÕES GERAL CONEXÃO
select
cid.nomedacidade "CIDADE", p.codigodaprogramacao "COD", p.nomedaprogramacao "PROGRAMAÇÃO",
case 
    when p.tipoprogramacao = 0 then 'TV'::text
    when p.tipoprogramacao = 1 then 'INTERNET'::TEXT
    when p.tipoprogramacao = 8 then 'TELEFONIA'::text
    else 'OUTROS'::text
end as "TIPO",
case
    when p.combo = 1 then 'SIM'::text
    else 'NÃO'::text
end as "COMBO",
tt.descricaotecnologia as "TECNOLOGIA",
case
    when p.tipoponto = 1 then 'PRINCIPAL'::TEXT
    when p.tipoponto = 2 then 'ADICIONAL'::text
    ELSE 'ANEXO'::text
end as "PONTO",
case 
    when p.tipoativacao = 1 then 'PRO-RATA'::TEXT
    when p.tipoativacao = 2 then 'PRÓX. MÊS'::TEXT
    else 'MÊS ATUAL CHEIO'::TEXT
end as "ATIVAÇÃO",
CASE
    when p.codextratificacao is null then 'NÃO TEM'
    else es.descricao 
end as "ESTRATIFICAÇÃO",
case
    when p.tier is null then 'NÃO TEM'::TEXT
    else i.nomeplano
end as "TIER PRIMÁRIO",
CASE 
    WHEN tp.codtier is null then 'NÃO TEM'::TEXT
    ELSE i2.nomeplano
end as "TIER SECUNDÁRIO",
case
     when p.liberadaparavenda = 1 then 'SIM'::text
    else 'NÃO'::text
end as "LIBERADO PARA VENDA",
array_to_string(ARRAY
         (
           SELECT pp.nome FROM programacaopacotesplay py
           JOIN programacaoplay pp on pp.codigoprogramacaoplay = py.codprogramacaoplay
           where py.codigoprogramacao = p.codigodaprogramacao and py.codigocidadeprogramacao = p.codcidade
         ), '-'::text) AS SVAS,
p.idcas as ID_CAS,
case 
    when p.tipoutilizacao = 1 then 'Telefonia'::text
    when p.tipoutilizacao = 2 then 'Comunicação de Dados'::text
    when p.tipoutilizacao = 3 then 'TV por Assinatura'::text
    when p.tipoutilizacao = 4 then 'Provimento de Acesso à Internet'::text
    when p.tipoutilizacao = 5 then 'Multimídia'::text
    else 'Outros'::text
end as "TIPO_UTILIZACAO"
from programacao p 
join cidade cid on cid.codigodacidade = p.codcidade 
LEFT join estratificacao es on es.codigo = p.codextratificacao
LEFT join tipotecnologiapacote tt on tt.codtipotecnologia = p.codtipotecnologia
LEFT join tiersprogramacao tp on tp.codcidade = p.codcidade and tp.codpacote = p.codigodaprogramacao
LEFT JOIN 
       (
         SELECT t.id,
                t.nomeplano
         FROM dblink(
           'hostaddr=10.0.0.183 dbname=ins user=postgres password=i745@postgres port=5432'
           ::text, 'SELECT i.id, i.nomeplano
                    from idhcp.planos i'::text) t(id bigint, nomeplano text)
       ) i ON i.id = p.tier
LEFT JOIN 
       (
         SELECT t.id,
                t.nomeplano
         FROM dblink(
           'hostaddr=150.230.79.177 dbname=ins user=postgres password=i745@postgres port=5432'
           ::text, 'SELECT i.id, i.nomeplano
                    from idhcp.planos i'::text) t(id bigint, nomeplano text)
       ) i2 ON i2.id = tp.codtier
--where cid.codigo_regional in (21,31,41,51,61) 
   --   and cid.codigodacidade not in (246001,266111,116221,1085391,1085441,1085411,
   --   121491,127931,133581,1085451,1085461,1085501,139781,139861,884121)
ORDER BY cid.nomedacidade, p.codigodaprogramacao, p.nomedaprogramacao asc

--------------------------------------------------------------------------------------------------------------------------------------------

-- SENHA MASTER
iAs@1nt3rf0cusm4st3r

--------------------------------------------------------------------------------------------------------------------------------------------

--VIEW SORTEIO TCM
with x as (
 SELECT ct.contrato
 FROM docreceber dr
      JOIN movimfinanceiro m ON m.numfatura = dr.fatura
      JOIN contratos ct ON ct.cidade = m.cidade AND ct.codempresa = m.codempresa AND ct.contrato = m.contrato
      JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
 WHERE dr.d_datavencimento BETWEEN '20220801' and '20220824' AND 
       dr.d_datapagamento IS NOT NULL AND
       ct.situacao = 2 AND 
       dr.situacao = 0 AND
       ct.d_datadainstalacao IS NOT NULL
)
SELECT DISTINCT cid.nomedacidade,
                ct.cidade,
                cli.codigocliente,
                cli.nome,
                ct.contrato,
                e.nomelogradouro,
                ct.d_datadainstalacao
FROM docreceber dr
JOIN movimfinanceiro m ON m.numfatura = dr.fatura
JOIN contratos ct ON ct.cidade = m.cidade AND ct.codempresa = m.codempresa AND ct.contrato = m.contrato
JOIN cidade cid ON cid.codigodacidade = ct.cidade
JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
JOIN enderecos e ON e.codigodologradouro = ct.enderecoconexao AND e.codigodacidade = ct.cidade
JOIN x ON x.contrato = ct.contrato
WHERE dr.d_datavencimento >= '20220801' AND
      ct.situacao = 2 AND
      dr.situacao = 0 AND
      cli.nome::text !~~ '%TESTE%' AND
      ct.d_datadainstalacao IS NOT NULL AND
      (ct.cidade = ANY (ARRAY [ 649471, 640411, 643941, 642381, 650991, 657221, 639671, 962961, 645481, 644831, 657901, 640761 ])) AND
      (ct.tipodocontrato <> ALL (ARRAY [ 31, 41, 71, 81, 91, 101, 141, 151, 161, 131 ]))

--------------------------------------------------------------------------------------------------------------------------------------------

-- VIEW RELATÓRIO PROTOCOLOS E OS SERVIÇOS CONEXÃO
CREATE OR REPLACE VIEW regrasoperacao.vis_protocolos_os_geral_historicos(
    protocolo,
    numos,
    contrato,
    cidade,
    "código_assi",
    nome,
    cpf_cnpj,
    tipo_historico,
    historico_pai,
    atendente,
    grupo_atendente,
    data_cadastro,
    hora_cadastro,
    data_fechamento,
    hora_fechamento,
    tempo_atendimento,
    grupo,
    assunto,
    telefone,
    "situação_contrato",
    status,
    razao_social,
    carteira,
    id_contrato,
    "situação_assunto")
AS
WITH s AS(
  SELECT hg_1.id,
         CASE
           WHEN hg_1.d_datafechamento IS NOT NULL THEN 1
           WHEN hg_1.d_datafechamento IS NULL AND hpai_1.d_datafechamento IS NOT NULL THEN 1
           ELSE 2
         END AS status
  FROM historicogeral hg_1
       LEFT JOIN historicogeral hpai_1 ON hpai_1.controle = hg_1.historicopai)
 SELECT DISTINCT 
        hg.controle AS protocolo,
        os.numos,
        ct.contrato,
        ci.nomedacidade AS cidade,
        cli.codigocliente AS "código_assi",
        cli.nome,
        cli.cpf_cnpj,
        CASE
          WHEN hg.historicopai IS NULL THEN 'Principal'::text
          ELSE 'Andamento'::text
        END AS tipo_historico,
        hg.historicopai AS historico_pai,
        hg.atendente,
        hga.namegroup AS grupo_atendente,
        hg.d_datacadastro AS data_cadastro,
        hg.t_horacadastro AS hora_cadastro,
        hg.d_datafechamento AS data_fechamento,
        hg.t_horafechamento AS hora_fechamento,
        CASE
          WHEN hg.d_datafechamento IS NOT NULL THEN (((hg.d_datafechamento || ' '::text) || hg.t_horafechamento)::timestamp without time zone) -(((
            hg.d_data || ' '::text) || hg.t_hora)::timestamp without time zone)
          WHEN hg.d_datafechamento IS NULL AND hpai.d_datafechamento IS NOT NULL THEN (((hpai.d_datafechamento || ' '::text) || hpai.t_horafechamento)
            ::timestamp without time zone) -(((hpai.d_data || ' '::text) || hpai.t_hora)::timestamp without time zone)
          ELSE NULL::interval
        END AS tempo_atendimento,
        translate(g.descricao::text, '.-;:,'::text, ','::text) AS grupo,
        translate(a.descricao::text, '.-:;,'::text, ','::text) AS assunto,
        func_retornatelefones(ct.cidade, ct.codigodocliente) AS telefone,
        v.descricaosituacao AS "situação_contrato",
        CASE
          WHEN s.status = 1 THEN 'fechado'::text
          ELSE 'aberto'::text
        END AS status,
        e.razaosocial AS razao_social,
        ca.descricao AS carteira,
        ct.id AS id_contrato,
        t.descricao AS "situação_assunto"
 FROM historicogeral hg
      JOIN ordemservico os ON os.cidade = hg.codigocidade and os.codempresa = hg.codempresa and os.numos = hg.ordemservico
      JOIN contratos ct ON ct.cidade = hg.codigocidade AND ct.codempresa = hg.codempresa AND ct.contrato = hg.codcontrato
      JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
      JOIN cidade ci ON ci.codigodacidade = ct.cidade
      JOIN empresas e ON e.codcidade = ct.cidade AND e.codempresa = ct.codempresa
      LEFT JOIN historicogeral hpai ON hpai.controle = hg.historicopai
      JOIN assuntohistorico a ON a.codigogrupo = hg.grupoassunto AND a.codigoassunto = hg.assunto
      JOIN grupohistorico g ON g.codigo = hg.grupoassunto
      LEFT JOIN usuariosdohistorico u ON u.controlehistorico = hg.controle
      LEFT JOIN hwusers hu ON lower(hu.login::text) = lower(u.usuario::text)
      LEFT JOIN hwgroups hgr ON hgr.id = hu.groupid
      LEFT JOIN hwusers hua ON lower(hua.login::text) = lower(hg.atendente::text)
      LEFT JOIN hwgroups hga ON hga.id = hua.groupid
      LEFT JOIN tiposituacaohistorico t ON t.codigo = hg.codigotiposituacao
      JOIN vis_situacaocontrato_descricao v ON v.situacao = ct.situacao
      JOIN carteira ca ON ca.codigo = ct.codcarteira
      JOIN s ON s.id = hg.id;
      
      
 where hg.d_datacadastro = '20220801'
 
 --------------------------------------------------------------------------------------------------------------------------------------------
 
 -- FUNÇÃO DICI CONEXÃO
 CREATE OR REPLACE FUNCTION public.funcao_dice_anatel_v4 (
  pdatadici date,
  ptipo smallint
)
RETURNS TABLE (
  tipo text,
  empresacoleta text,
  anocoleta integer,
  mescoleta integer,
  municipioibge text,
  tipocliente text,
  tipoatendimento text,
  tipomeioacesso text,
  tecnologia text,
  tipoproduto text,
  velocidadecontratada numeric,
  codigocidade bigint,
  nomecidade text,
  ufcidade text,
  downstream bigint,
  quantidade bigint
) AS
$body$
begin
  /*
    Parâmetros:
    pTipo: [1]Internet [2]TV
  */
  return query
    with
    doc as (
      select d.id, d.fatura, ct.id as idcontrato, cli.id as idassinante,
      (
        select m.id from 
        public.movimfinanceiro m
        join public.programacao pr on pr.codcidade = m.cidade and pr.codigodaprogramacao = m.numerodaprogramacao
        where m.numfatura = d.fatura and (m.tipoprogramacao = 0 or pr.combo = 1)
        order by m.qtdediascobranca desc limit 1
      ) as idtv,
      (
        select m.id 
        from public.movimfinanceiro m
        join public.programacao pr on pr.codcidade = m.cidade and pr.codigodaprogramacao = m.numerodaprogramacao
        where m.numfatura = d.fatura and (m.tipoprogramacao = 1 or pr.combo = 1)
        order by m.qtdediascobranca desc limit 1
      ) as idinternet
      from public.docreceber d
      join public.fatura f on f.numerofatura = d.fatura
      join public.contratos ct on ct.cidade = f.codigodacidade and ct.codempresa = f.codempresa and ct.contrato = f.numerodocontrato
      join public.cidade cid on cid.codigodacidade = d.codigodacidade
      join public.clientes cli on cli.cidade = d.codigodacidade and cli.codigocliente = d.cliente
      where d.d_datafaturamento between to_char(pdatadici,'YYYY-MM-01')::date
        and (to_char(pdatadici,'YYYY-MM-01')::date + interval '1 month')::date - 1
        and (d.reparcelamento is null or d.reparcelamento = 0)
        and (d.boletoequipamento is null or d.boletoequipamento = 0)
        and (ct.codcarteira = any(array[31, 41, 61, 71, 81, 91]))
    ),
    pla as (
      select * from public.func_retorna_planos_todos_ins()
    )
    select t.*, count(*) from (
      select
       case when pTipo = 2 then 'TV' else 'Internet' end as tipo,
     case
       		when ct.codcarteira = 31 then '07.054.341/0001-99'
            when ct.codcarteira = 41 then '16.753.142/0001-60'
            when ct.codcarteira = 61 then '05.012.742/0001-50'
            when ct.codcarteira = 71 then '05.012.742/0001-50'
            when ct.codcarteira = 81 then '24.488.226/0001-41'
            when ct.codcarteira = 91 then '05.539.629/0001-28'
            else null
            end as empresacoleta,
         extract(year from pdatadici)::integer as anocoleta,
        extract(month from pdatadici)::integer as mescoleta,
        t.codigoibge::text as municipioibge,
        case when length(translate(cli.cpf_cnpj,E'.,://\\- ',E'')) = 14 then 'PJ'::text else 'PF'::text end as tipocliente,
        'Urbano'::text as tipoatendimento,
        tec.descricaotecnologia::text as tipomeioacesso,
        tec.descricaotecnologia::text as tipotecnologia,
        case when pTipo = 2 then 'TV'::text else 'Internet'::text end as tipoproduto,
        case when pla.downstream is null then 0::numeric
             when pla.downstream >= 1000000 then round((pla.downstream::numeric / 1000 / 1000),2)
             else round(pla.downstream::numeric / 1000,2)
        end as velocidadecontratada,
        t.codigo::bigint as codigodacidade, t.nome::text as nomecidade, t.estado::text as ufcidade, pla.downstream
      from doc
      join public.movimfinanceiro m on case when pTipo = 1 then m.id = doc.idinternet else m.id = doc.idtv end
      join public.contratos ct on ct.id = doc.idcontrato
      join public.clientes cli on cli.id = doc.idassinante
      join public.tablocal t on t.codigo = cli.cidade
      join public.sistemasintegrados si on si.codcidade = t.codigo and lower(si.nomesistema) = 'ins'
      join public.programacao pr on pr.codcidade = m.cidade and pr.codigodaprogramacao = m.numerodaprogramacao
      left join public.tipotecnologiapacote tec on tec.codtipotecnologia = pr.codtipotecnologia
      left join pla on pla.idPlano = pr.tier and pla.insIp = si.ipservidor
  ) as t
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;
end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100 ROWS 1000;

ALTER FUNCTION public.funcao_dice_anatel_v4 (pdatadici date, ptipo smallint)
  OWNER TO postgres;
  
--------------------------------------------------------------------------------------------------------------------------------------------

-- RELATÓRIO DE CONFRONTO ENTRE OS PACOTES BNT BRASILNET - 29055
select pr.codcidade, cid.nomedacidade, pr.codigodaprogramacao, pr.nomedaprogramacao,
  e.codigo as codigoestratificacao, e.descricao as nomeestratificacao,
  ep.descricao as nomeproduto, ei.valoraplicado,
  case when ei.tipovalor = 1 then 'R$' when ei.tipovalor = 2 then '%' when ei.tipovalor = 3 then 'Resto' end as tipovalor
from public.programacao pr
join public.cidade cid on cid.codigodacidade = pr.codcidade
left join public.estratificacao e on e.codigo = pr.codextratificacao
left join public.estratificacao_itens ei on ei.codigoestratificacao = e.codigo
left join public.estratificacao_produtos ep on ep.codigo = ei.codprodutoestratificacao
where pr.codprodutoextratificacao is null

--------------------------------------------------------------------------------------------------------------------------------------------

-- VIEW LUCIANA SFERANET
SELECT DISTINCT
c.nomedacidade AS "Cidade",
cl.codigocliente AS "Codigo_Cliente",
cl.nome AS "Nome",
cl.cpf_cnpj AS "Cpf_Cnpj",
func_retornatelefones(cl.cidade, cl.codigocliente) AS "Telefone",
ct.contrato AS "Contrato",
ed.nomelogradouro AS "Endereco_Conexao",
ct.numeroconexao AS "Numero_Conexao",
ct.cepconexao AS "Cep_Conexao",
ct.bairroconexao AS "Bairro_Conexao",
CASE
WHEN ct.situacao = 1 THEN 'Aguardando Conexão'
WHEN ct.situacao = 2 THEN 'Conectado'
WHEN ct.situacao = 3 THEN 'Pausado'
WHEN ct.situacao = 4 THEN 'Inadimplente'
WHEN ct.situacao = 5 THEN 'Cancelado'
WHEN ct.situacao = 6 THEN 'Endereço não Cabeado'
WHEN ct.situacao = 7 THEN 'Conectado/Inadimplente'
END AS Situacao_Contrato,
dr.d_datafaturamento AS "Data_Fatauramento",
dr.numerodocumento AS "Numero_Documento",
array_to_string(
ARRAY(
select distinct mf.observacao
from public.movimfinanceiro mf
where
mf.numfatura = dr.fatura
order by mf.observacao
),'- '
) AS "Observação",
dr.valordocumento AS "Valor_Documento",
dr.d_datavencimento AS "Data_Vencimento"
FROM docreceber dr
JOIN public.fatura f ON f.numerofatura = dr.fatura
JOIN public.contratos ct ON ct.cidade = f.codigodacidade and ct.contrato = f.numerodocontrato
JOIN public.clientes cl ON cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente
JOIN public.cidade c ON c.codigodacidade = cl.cidade
JOIN public.movimfinanceiro m ON m.numfatura = dr.fatura
--join public.vis_contratos_cancelados_relatorios ctc on ctc.id = ct.id
JOIN public.enderecos ed ON ed.codigodacidade = ct.cidade and ed.codigodologradouro = ct.enderecoconexao
WHERE dr.d_datavencimento BETWEEN '20220716' and '20220716' and
dr.situacao = 0 and
dr.d_datapagamento IS NULL and
ct.situacao IN (4,7)
ORDER BY c.nomedacidade

--------------------------------------------------------------------------------------------------------------------------------------------

--Solicitação de Dados Cadastrais - Embratel CONECTCOR - 29909
with ins as (
select a.idcliente, a.numerointerno
from dblink(
'hostaddr=186.193.65.26 dbname=ins user=postgres password=i745@postgres port=5432'::text, 
'select cl.idcliente, vp.numerointerno
from idhcp.voip vp
JOIN idhcp.clientes cl ON cl.id = vp.id
where vp.numerointerno IN (''551434182622'',''551436550565'',''551432252510'',''551432252985'',''551432252694'',''551434164047'',''551432251212'',''551432250979'',''551436225653'',''551432252065'',''551432250608'',''551436525016'',''551436551357'',
''551436550741'',''551432252123'',''551432252145'',''551432250047'',''551432251756'',''551432251232'',''551432250171'',''551432251700'',''551436141100'',''551436211737'',''551432251619'',''551436550762'',''551436550365'',
''551432252047'',''551436351170'',''551432250609'',''551432250060'',''551432250225'',''551436245110'',''551432251462'',''551432250157'',''551432250177'',''551432250577'',''551432250634'',''551432252193'',''551436244106'',
''551436143210'',''551436242126'',''551436550089'',''551434351066'',''551436141701'',''551436342642'',''551432251262'',''551432250409'',''551436147114'',''551432250338'',''551432250385'',''551432252188'',''551432250527'',
''551432251376'',''551432250251'',''551436214788'',''551432250621'',''551436521178'',''551436145529'',''551436551378'',''551436217829'',''551436550181'',''551432250132'',''551436552345'',''551432250062'',''551436554939'',
''551436144884'',''551436340547'',''551436245312'',''551432250154'',''551436141995'',''551436351475'',''551436550659'',''551421043016'',''551432252508'',''551432250102'',''551432251904'',''551432250375'',''551436550625'',
''551436351647'',''551432251333'',''551432251382'',''551432250499'',''551436553210'',''551432250227'',''551436291934'',''551436255037'',''551436140659'',''551432250192'',''551436226354'',''551432252820'',''551436141168'',
''551432252720'',''551436550424'',''551432251814'')'::text) a (idcliente bigint, numerointerno character varying)
),
  canc as (
    select ct.id, max(ord.d_dataexecucao) as cancelamento 
    from ordemservico ord
    join lanceservicos l on l.codigodoserv_lanc=ord.codservsolicitado
    join contratos ct on ct.cidade=ord.cidade and ct.codempresa=ord.codempresa and ct.contrato=ord.codigocontrato
    where l.situacaocontrato = 5
    group by ct.id
  )
select distinct /*t.ddd||translate(t.telefone,' -./','') as telefone,*/ ct.id, cli.nome, cli.cpf_cnpj, ct.d_datadainstalacao, ca.cancelamento as desativacao, 
e.tipodologradouro, e.nomelogradouro, ct.numeroconexao, ct.complementoconexao, ct.bairroconexao, cid.nome, cid.estado, ct.cepconexao, ec.tipodologradouro, 
ec.nomelogradouro, ct.numerocobranca, ct.complementocobranca, ct.bairrocobranca, tc.nome, tc.estado, ct.cepcobranca
from clientes cli
join contratos ct on ct.cidade = cli.cidade and ct.codigodocliente = cli.codigocliente
join vis_situacaocontrato_descricao v on v.situacao = ct.situacao
join telefones t on t.cidade = cli.cidade and t.codigocliente = cli.codigocliente
join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
join tablocal cid on cid.codigo = ct.cidade
join enderecos ec on ec.codigodacidade = ct.cidadecobranca and ec.codigodologradouro = ct.enderecodecobranca
join tablocal tc on tc.codigo = ct.cidadecobranca
join ins i on i.idcliente = ct.id
left join canc ca on ca.id = ct.id

--------------------------------------------------------------------------------------------------------------------------------------------

-- TO CHAR DATA DATE
select to_char("Data desativacao"::date,'DD-MM-YYYY')::date, * from relatoriospersonalizados.funcao_ias_usuarios()

--------------------------------------------------------------------------------------------------------------------------------------------

-- VERIFICA O TIPO DE LANÇAMENTO DO BOLETO
select m.lanc_servico, l.descricaodoserv_lanc from docreceber dr
JOIN movimfinanceiro m ON m.numfatura = dr.fatura
JOIN lanceservicos l ON l.codigodoserv_lanc = m.lanc_servico
where dr.nossonumero IN (
'8497480',
'8499232',
'8494831',
'8196653',
'8116899',
'8116900',
'8177312',
'8209304',
'8209305',
'8199350',
'8199351')

--------------------------------------------------------------------------------------------------------------------------------------------

-- VERIFICA SE FOI GERADA NOTA FISCAL PELO NOSSO NÚMERO
select * from docreceber dr
JOIN itensnf i ON i.numfatura = dr.fatura
where dr.nossonumero IN (
'8497480',
'8499232',
'8494831',
'8196653',
'8116899',
'8116900',
'8177312',
'8209304',
'8209305',
'8199350',
'8199351')

--------------------------------------------------------------------------------------------------------------------------------------------

-- PEGAR INS BASES
select * from sistemasintegrados

--------------------------------------------------------------------------------------------------------------------------------------------

--ARRUMA ERRO LOGIN HWUSERS
INSERT INTO hwusers  (nameuser,login,user_password,groupid,d_blockdate) VALUES ('MIYOSHITADEU','MIYOSHITADEU','123123a',1,NULL );

update hwusers h set user_password = '123123a' where h.login = 'PEDROVICENTE';

--------------------------------------------------------------------------------------------------------------------------------------------

--VIEW API ORDEMSERVICOSIMPLIFICADA GETODATA CCOM
CREATE VIEW centralassinante.vis_ordemservico_web_simplificada (
    id,
    codcidade,
    assinanteid,
    contratoid,
    servico,
    nomeequipe,
    dataagendamento,
    dataatendimento,
    dataconclusao,
    numos,
    situacao,
    observacao)
AS
SELECT os.id,
    cli.cidade AS codcidade,
    cli.id AS assinanteid,
    ct.id AS contratoid,
    l.descricaodoserv_lanc AS servico,
    e.nomedaequipe AS nomeequipe,
    os.d_dataagendamento AS dataagendamento,
    os.d_dataatendimento + COALESCE(os.t_horaatendimento, '00:00:00'::time without time zone) AS dataatendimento,
    os.d_databaixa + COALESCE(os.t_horabaixa, '00:00:00'::time without time zone) AS dataconclusao,
    os.numos,
    os.situacao,
    os.observacoes::text AS observacao
FROM ordemservico os
     JOIN lanceservicos l ON l.codigodoserv_lanc = os.codservsolicitado
     JOIN contratos ct ON ct.cidade = os.cidade AND ct.codempresa = os.codempresa AND ct.contrato = os.codigocontrato
     JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
     JOIN equipe e ON e.codigocidade = os.cidade AND e.codigodaequipe = os.equipe
WHERE os.d_datacadastro > ('now'::text::date - ((
    SELECT pcv.valor::integer AS valor
    FROM centralassinante.parametrochavevalor pcv
             JOIN centralassinante.parametro p ON p.id = pcv.parametroid
    WHERE p.ativo AND pcv.chave::text = 'PeriodoMinimoAtendimento'::text
    ))) AND (l.codigodoserv_lanc IN (
    SELECT regexp_split_to_table(pcv.valor::text, ','::text)::integer AS regexp_split_to_table
    FROM centralassinante.parametrochavevalor pcv
             JOIN centralassinante.parametro p ON p.id = pcv.parametroid
    WHERE p.ativo AND pcv.chave::text = 'CodigosServicosListagemOrdemServico'::text
    ))
UNION
SELECT ('10000'::text || hg.id::text)::bigint AS id,
    c.cidade AS codcidade,
    c.id AS assinanteid,
    ct.id AS contratoid,
    ah.descricao AS servico,
    'CENTRAL DE ATENDIMENTO'::text AS nomeequipe,
    NULL::date AS dataagendamento,
    hg.d_data + COALESCE(hg.t_hora, '00:00:00'::time without time zone) AS dataatendimento,
    hg.d_datafechamento + COALESCE(hg.t_horafechamento, '00:00:00'::time without time zone) AS dataconclusao,
    hg.id AS numos,
        CASE
            WHEN hg.d_datafechamento IS NULL THEN 1
            ELSE 3
        END AS situacao,
    hg.descricao AS observacao
FROM historicogeral hg
     JOIN assuntohistorico ah ON hg.assunto = ah.codigoassunto AND hg.grupoassunto = ah.codigogrupo
     JOIN clientes c ON hg.codigocidade = c.cidade AND hg.assinante = c.codigocliente
     JOIN contratos ct ON hg.codigocidade = ct.cidade AND hg.codcontrato = ct.contrato
WHERE hg.d_datacadastro > ('now'::text::date - ((
    SELECT pcv.valor::integer AS valor
    FROM centralassinante.parametrochavevalor pcv
             JOIN centralassinante.parametro p ON p.id = pcv.parametroid
    WHERE p.ativo AND pcv.chave::text = 'PeriodoMinimoAtendimento'::text
    ))) AND (ah.codigoassunto IN (
    SELECT regexp_split_to_table(pcv.valor::text, ','::text)::integer AS regexp_split_to_table
    FROM centralassinante.parametrochavevalor pcv
             JOIN centralassinante.parametro p ON p.id = pcv.parametroid
    WHERE p.ativo AND pcv.chave::text = 'CodigosAssuntoHistoricoPermitidos'::text
    )) AND NOT (hg.controle IN (
    SELECT
                CASE
                    WHEN gh.historicopai IS NULL THEN gh.controle
                    ELSE gh.historicopai
                END AS numhistorico
    FROM ordemservico os
             JOIN historicogeral gh ON gh.controle = os.numhistorico
    ));

ALTER VIEW centralassinante.vis_ordemservico_web_simplificada
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

--FUNÇÕES RELATÓRIO MF HILTON
--CRIAR SERVER
CREATE SERVER srvIns
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host '187.111.170.18', dbname 'ins');
--USUÁRIO PARA SRVINS
CREATE USER MAPPING FOR PUBLIC  -- Se for para todos os usuários, mudar o CURRENT_USER para PUBLIC
  SERVER srvins
  OPTIONS (user 'postgres', password 'i745@postgres');
--CRIAR INSFDW
create schema insfdw;
IMPORT FOREIGN SCHEMA idhcp
  limit to (equipamentos,planos)
  FROM SERVER srvins
  INTO insfdw;
--ALTERAR TIPOS DE RUA
update public.enderecos set tipodologradouro = 'ALAMEDA' where tipodologradouro = 'AL.';
update public.enderecos set tipodologradouro = 'AVENIDA' where tipodologradouro in ('AV','AV.','AV:');
update public.enderecos set tipodologradouro = 'RODOVIA' where tipodologradouro = 'ROD.';
update public.enderecos set tipodologradouro = 'PROFESS.' where tipodologradouro = 'PROF.';
--SELECT CIDADES
select * from public.cidade;


--DADOS CADASTRAIS
CREATE OR REPLACE FUNCTION relatoriospersonalizados.func_rel_dados_cadastrais (
)
RETURNS TABLE (
"cnpj_cpf" text,
"cliente_nome" varchar,
"cliente_razao_social" text,
"doc_ie_rg" varchar,
"doc_inscricao_municipal" text,
"doc_outro" text,
"data_nascimento" text,
"ind_sexo" text,
"ind_pf_pj" text,
"endereco_fat_logr_tipo" varchar,
"endereco_fat_logr_nom" varchar,
"endereco_fat_nro" varchar,
"endereco_fat_bairro" varchar,
"endereco_fat_cidade" varchar,
"endereco_fat_uf" varchar,
"endereco_fat_cep" varchar,
"cliente_email" varchar,
"telefone_1" text,
"telefone_2" text,
"telefone_3" text,
"telefone_4" text,
"cliente_nome_mae" varchar,
"cliente_nome_pai" varchar
) AS
$body$
begin
	Create temporary table temp_rel_dados_cadastrais(
    "cnpj_cpf" text,
    "cliente_nome" varchar(40),
    "cliente_razao_social" text,
    "doc_ie_rg" varchar(18),
    "doc_inscricao_municipal" text,
    "doc_outro" text,
    "data_nascimento" text,
    "ind_sexo" text,
    "ind_pf_pj" text,
    "endereco_fat_logr_tipo" varchar(32),
    "endereco_fat_logr_nom" varchar(71),
    "endereco_fat_nro" VARCHAR(10),
    "endereco_fat_bairro" varchar(20),
    "endereco_fat_cidade" varchar(30),
    "endereco_fat_uf" varchar(2),
    "endereco_fat_cep" varchar(8),
    "cliente_email" varchar(200),
    "telefone_1" text,
    "telefone_2" text,
    "telefone_3" text,
    "telefone_4" text,
    "cliente_nome_mae" varchar(50),
    "cliente_nome_pai" varchar(50)
    ) On commit drop;
    
    insert into temp_rel_dados_cadastrais
  with
ass as (
  select translate(cli.cpf_cnpj,E'.,//\\- ',E''), min(cli.id) as menor
  from clientes cli
  JOIN contratos ct ON ct.codigodocliente = cli.codigocliente AND ct.cidade = cli.cidade
  where ct.situacao IN (2,3)
  group by translate(cli.cpf_cnpj,E'.,//\\- ',E'')
)
select translate(cli.cpf_cnpj::text, '.-/'::text, ''::text) as cnpj_cpf,
       replace(cli.nome,';','') as cliente_nome,
       CASE WHEN length(translate(cli.cpf_cnpj::text, E'.-//\\- '::text, E''::text)) > 11 THEN replace(cli.nome,';','')
        ELSE ''
       END AS cliente_razao_social,
       replace(cli.inscrest_rg,';','') as doc_ie_rg,
       '' as DOC_INSCRICAO_MUNICIPAL,
       '' as DOC_OUTRO,
       to_char(cli.d_datanascimento,'YYYY-MM-DD') as data_nascimento,
       CASE WHEN cli.sexo = 1 THEN 'M' ELSE 'F' END AS IND_SEXO,
       CASE WHEN length(translate(cli.cpf_cnpj::text, E'.-//\\- '::text,''::text)) > 11 THEN 'PJ'::text
        ELSE 'PF'::text
       END AS IND_PF_PJ,
       replace(ed.tipodologradouro,';','') as ENDERECO_FAT_LOGR_TIPO,
       replace(ed.nomelogradouro,';','') as ENDERECO_FAT_LOGR_NOM,
       coalesce(regexp_replace(cli.numeroresidencial, '[^0-9]', '', 'gi'),'00') as ENDERECO_FAT_NRO,
       coalesce(replace(cli.bairrocobranca,';',''),'NAO INFORMADO') as ENDERECO_FAT_BAIRRO,
       replace(cid.nomedacidade,';','') as ENDERECO_FAT_CIDADE,
       replace(tab.estado,';','') as ENDERECO_FAT_UF,
       cli.cepcobranca as ENDERECO_FAT_CEP,
       replace(cli.email,',',';') as CLIENTE_EMAIL,
       regexp_replace(split_part(func_retornatelefones(cli.cidade, cli.codigocliente), '/'::text, 1), '[^0-9]', '', 'gi') as telefone_1,
       regexp_replace(split_part(func_retornatelefones(cli.cidade, cli.codigocliente), '/'::text, 2), '[^0-9]', '', 'gi') as telefone_2,
       regexp_replace(split_part(func_retornatelefones(cli.cidade, cli.codigocliente), '/'::text, 3), '[^0-9]', '', 'gi') as telefone_3,
       regexp_replace(split_part(func_retornatelefones(cli.cidade, cli.codigocliente), '/'::text, 4), '[^0-9]', '', 'gi') as telefone_4,
       replace(cli.nomemae,';','') as CLIENTE_NOME_MAE,
       replace(cli.nomepai,';','') as CLIENTE_NOME_PAI
from clientes cli
join ass on ass.menor = cli.id
JOIN cidade cid on cid.codigodacidade = cli.cidade
JOIN tablocal tab ON tab.codigo = cli.cidade
JOIN enderecos ed ON ed.codigodacidade = cli.cidade and ed.codigodologradouro = cli.enderecoresidencial;
return query select * from temp_rel_dados_cadastrais;
end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100 ROWS 1000;

ALTER FUNCTION relatoriospersonalizados.func_rel_dados_cadastrais ()
  OWNER TO postgres;

--DADOS CONTRATOS
CREATE OR REPLACE FUNCTION relatoriospersonalizados.func_rel_dados_contratos (
)
RETURNS TABLE (
"CNPJ_CPF" text,
"COD_CONTRATO" integer,
"EMAIL_CONTRATO" varchar,
"DATA_CONTRATO" text,
"DATA_ATIVACAO" text,
"DATA_INICIO_COBRANCA" text,
"PERIODO_VIGENCIA" text,
"VALOR_INSTALACAO" text,
"VALOR_CONTRATO" text,
"SITUACAO_CONTRATO" text,
"DESIGNACAO_CONTRATO" text,
"DIA_DE_VENCIMENTO" integer,
"DATA_DO_ULTIMO_BLOQUEIO" date,
"FORMA_PAGAMENTO" text
) AS
$body$
begin
	Create temporary table temp_rel_dados_contratos(
    "CNPJ_CPF" text,
    "COD_CONTRATO" integer,
    "EMAIL_CONTRATO" varchar(200),
    "DATA_CONTRATO" text,
    "DATA_ATIVACAO" text,
    "DATA_INICIO_COBRANCA" text,
    "PERIODO_VIGENCIA" text,
    "VALOR_INSTALACAO" text,
    "VALOR_CONTRATO" text,
    "SITUACAO_CONTRATO" text,
    "DESIGNACAO_CONTRATO" text,
    "DIA_DE_VENCIMENTO" integer,
    "DATA_DO_ULTIMO_BLOQUEIO" date,
    "FORMA_PAGAMENTO" text
    ) On commit drop;
    
    insert into temp_rel_dados_contratos
select translate(cli.cpf_cnpj::text, E'.-//\\- '::text, E''::text) as cnpj_cpf,
       cp.id as COD_CONTRATO,
       replace(cli.email,',',';') as EMAIL_CONTRATO,
       to_char(ct.d_datadavenda,'YYYY-MM-DD') as DATA_CONTRATO,
       to_char(ct.d_datadainstalacao,'YYYY-MM-DD') as DATA_ATIVACAO,
       to_char(ct.d_datadainstalacao,'YYYY-MM-DD') as DATA_INICIO_COBRANCA,
       '' as PERIODO_VIGENCIA,
       btrim(to_char(ct.valordocontrato,'999999999990D00')) as VALOR_INSTALACAO,
       btrim(to_char(cp.valorpacote,'999999999990D00')) as VALOR_CONTRATO,
         CASE WHEN ct.situacao = 1 THEN 'EM ATIVACAO'
            WHEN ct.situacao = 2 THEN 'ATIVO'
            WHEN ct.situacao = 3 THEN 'BLOQUEADO'
       END AS situacao_contrato,
       '' as designacao_contrato,
       ct.dtvencto as DIA_DE_VENCIMENTO,
       cp.d_datadesativacao as DATA_DO_ULTIMO_BLOQUEIO,
       CASE WHEN ct.formapagamento = 1 THEN 'BOLETO'
            WHEN ct.formapagamento = 2 THEN 'DEPOSITO'
            WHEN ct.formapagamento = 3 THEN 'DEBITO EM CONTA'
       ELSE 'CARTAO'
       END AS FORMA_PAGAMENTO
from contratos ct
JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
LEFT JOIN cont_prog cp ON cp.cidade = ct.cidade AND cp.codempresa = ct.codempresa AND cp.contrato = ct.contrato
where ct.situacao IN (2,3);
return query select * from temp_rel_dados_contratos;
end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100 ROWS 1000;

ALTER FUNCTION relatoriospersonalizados.func_rel_dados_contratos ()
  OWNER TO postgres;

--ITENS CONTRATO
CREATE OR REPLACE FUNCTION relatoriospersonalizados.func_rel_itens_contratos (
)
RETURNS TABLE (
"COD_CONTRATO" bigint,
"QUANTIDADE_ITEM" integer,
"VALOR_UNITARIO" text,
"VALOR_TOTAL" text,
"ENDERECO_INSTALACAO_LOGR_TIPO" varchar,
"ENDERECO_INSTALACAO_LOGR_NOM" varchar,
"ENDERECO_INSTALACAO_NRO" text,
"ENDERECO_INSTALACAO_COMPL" text,
"ENDERECO_INSTALACAO_BAIRRO" text,
"ENDERECO_INSTALACAO_CIDADE" varchar,
"ENDERECO_INSTALACAO_UF" varchar,
"ENDERECO_INSTALACAO_CEP" varchar,
"COD_MAC_ADDRESS" text,
"COD_SERIAL_NUMBER" text,
"DESIGNACAO_TECNICA" text,
"WIFI_NOME" text,
"WIFI_SENHA" text,
"PPPOE_USUARIO" varchar,
"PPPOE_SENHA" varchar,
"NUMERO_IP_POP" text,
"NUMERO_SLOT" text,
"NUMERO_PON" text,
"TIPO_ROTEAMENTO" text,
"NOME_PRODUTO" varchar,
"COD_POLICY" varchar,
"NRO_LINHA" text,
"COD_POLICY_DOWN" bigint,
"COD_POLICY_UP" bigint,
"ID" bigint,
"D_DATAFATURAMENTO" date,
"SITUACAO" smallint
) AS
$body$
begin
return query
with
eq as (
  select e1.idcliente, e1.username, e1.senha, pl.upstream, pl.downstream, pl.nomeplano
  from (
    select e.idcliente, max(e.id) as maior
    from insfdw.equipamentos e
    where e.idmotivosderetirada is null and e.tipoequipamento = 3
    group by e.idcliente
  ) as t
  join insfdw.equipamentos e1 on e1.id = t.maior
  join insfdw.planos pl on pl.id = e1.idplano
)
select
  cp.id as COD_CONTRATO,
  '1'::integer as QUANTIDADE_ITEM,
  btrim(to_char(cp.valorpacote,'999999999990D00')) as VALOR_UNITARIO,
  btrim(to_char(cp.valorpacote,'999999999990D00')) as VALOR_TOTAL,
  ed.tipodologradouro as ENDERECO_INSTALACAO_LOGR_TIPO,
  ed.nomelogradouro as ENDERECO_INSTALACAO_LOGR_NOM,
  coalesce(regexp_replace(ct.numeroconexao, '[^0-9]', '', 'gi'),'00') as ENDERECO_INSTALACAO_NRO,
  replace(ct.complementoconexao,';','') as ENDERECO_INSTALACAO_COMPL,
  replace(ct.bairroconexao,';','') as ENDERECO_INSTALACAO_BAIRRO,
  cid.nomedacidade as ENDERECO_INSTALACAO_CIDADE,
  tab.estado as ENDERECO_INSTALACAO_UF,
  ct.cepconexao as ENDERECO_INSTALACAO_CEP,
  ''::text as COD_MAC_ADDRESS,
  ''::text as COD_SERIAL_NUMBER,
  ''::text as DESIGNACAO_TECNICA,
  ''::text as WIFI_NOME,
  ''::text as WIFI_SENHA,
  eq.username as PPPOE_USUARIO,
  eq.senha as PPPOE_SENHA,
  ''::text as NUMERO_IP_POP,
  ''::text as NUMERO_SLOT,
  ''::text as NUMERO_PON,
  ''::text as TIPO_ROTEAMENTO,
  pr.nomedaprogramacao as NOME_PRODUTO,
  eq.nomeplano as COD_POLICY,
  ''::text as NRO_LINHA,
  eq.downstream as COD_POLICY_DOWN,
  eq.upstream as COD_POLICY_UP, ct.id, ct.d_datafaturamento, ct.situacao
from cont_prog cp
join cidade cid on cp.cidade = cid.codigodacidade
join contratos ct on ct.cidade = cp.cidade and ct.contrato = cp.contrato and ct.codempresa = cp.codempresa
JOIN enderecos ed ON ed.codigodacidade = ct.cidade and ed.codigodologradouro = ct.enderecoconexao
JOIN tablocal tab ON tab.codigo = ct.cidade
join programacao pr on pr.codcidade = cp.cidade and pr.codigodaprogramacao = cp.protabelaprecos
left join eq on eq.idcliente = ct.id
where ct.situacao IN (2,3);
end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100 ROWS 1000;

ALTER FUNCTION relatoriospersonalizados.func_rel_itens_contratos ()
  OWNER TO postgres;

--DADOS FINANCEIRO
CREATE OR REPLACE FUNCTION relatoriospersonalizados.func_rel_dados_financeiro (
pData date
)
RETURNS TABLE (
"COD_CONTRATO" integer,
"DATA_EMISSAO" date,
"DATA_VENCIMENTO" date,
"VALOR" text,
"STATUS" text,
"NRO_TITULO" varchar,
"FORMA_PAGAMENTO" text,
"NRO_BOLETO" varchar
) AS
$body$
begin
	Create temporary table temp_rel_dados_financeiro(
    "COD_CONTRATO" integer,
    "DATA_EMISSAO" date,
    "DATA_VENCIMENTO" date,
    "VALOR" text,
    "STATUS" text,
    "NRO_TITULO" varchar(20),
    "FORMA_PAGAMENTO" text,
    "NRO_BOLETO" varchar(20)
    ) On commit drop;
    
    insert into temp_rel_dados_financeiro
with
con as (
select translate(cli.cpf_cnpj,E'.,//\\- ',E'') as cpf, ct.id, min(cp.id) as contrato
from clientes cli
JOIN contratos ct ON ct.codigodocliente = cli.codigocliente AND ct.cidade = cli.cidade
join public.cont_prog cp on cp.cidade = ct.cidade and cp.codempresa = ct.codempresa and cp.contrato = ct.contrato
where ct.situacao IN (2,3)
group by translate(cli.cpf_cnpj,E'.,//\\- ',E''), ct.id
)
SELECT DISTINCT
   con.contrato as COD_CONTRATO,
   dr.d_datacadastro as DATA_EMISSAO,
   dr.d_datavencimento as DATA_VENCIMENTO,
   btrim(to_char(dr.valordocumento,'99999999990D00')) as VALOR,
   'Aberto' as STATUS,
   dr.numerodocumento as NRO_TITULO,
   CASE
    WHEN dr.formadepagamento = 1 THEN 'BOLETO'
    WHEN dr.formadepagamento = 2 THEN 'DEPOSITO'
    WHEN dr.formadepagamento = 3 THEN 'DEBITO EM CONTA'
    WHEN dr.formadepagamento = 4 THEN 'CARTAO'
   END AS FORMA_PAGAMENTO,
   dr.numerodocumento as NRO_BOLETO
FROM docreceber dr
 JOIN cidade c ON c.codigodacidade = dr.codigodacidade
 JOIN public.fatura f ON f.numerofatura = dr.fatura
 jOIN public.contratos ct ON ct.cidade = f.codigodacidade and ct.contrato = f.numerodocontrato and ct.codempresa = f.codempresa
 JOIN public.clientes cl ON cl.cidade = dr.codigodacidade and cl.codigocliente = dr.cliente
 join con on con.id = ct.id
 where ct.situacao IN (2,3) and dr.situacao = 0 and dr.d_datavencimento >= pData
 order by con.contrato, dr.numerodocumento;
return query select * from temp_rel_dados_financeiro;
end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100 ROWS 1000;

ALTER FUNCTION relatoriospersonalizados.func_rel_dados_financeiro (pData date)
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

-- VIEW API SWAGGER ORDEMSERVICO GETODATA
CREATE OR REPLACE VIEW centralassinante.vis_ordemservico_web_simplificada(
    id,
    codcidade,
    assinanteid,
    contratoid,
    servico,
    nomeequipe,
    dataagendamento,
    dataatendimento,
    dataconclusao,
    numos,
    situacao,
    observacao)
AS
SELECT os.id,
         cli.cidade AS codcidade,
         cli.id AS assinanteid,
         ct.id AS contratoid,
         l.descricaodoserv_lanc AS servico,
         e.nomedaequipe AS nomeequipe,
         os.d_dataagendamento AS dataagendamento,
         os.d_dataatendimento + COALESCE(os.t_horaatendimento, '00:00:00'::time without time zone) AS dataatendimento,
         os.d_databaixa + COALESCE(os.t_horabaixa, '00:00:00'::time without time zone) AS dataconclusao,
         os.numos,
         os.situacao,
         os.observacoes::text AS observacao
  FROM ordemservico os
       JOIN lanceservicos l ON l.codigodoserv_lanc = os.codservsolicitado
       JOIN contratos ct ON ct.cidade = os.cidade AND ct.codempresa = os.codempresa AND ct.contrato = os.codigocontrato
       JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
       JOIN equipe e ON e.codigocidade = os.cidade AND e.codigodaequipe = os.equipe
  WHERE os.d_datacadastro >('now'::text::date -((
                                                  SELECT pcv.valor::integer AS valor
                                                  FROM centralassinante.parametrochavevalor pcv
                                                       JOIN centralassinante.parametro p ON p.id = pcv.parametroid
                                                  WHERE p.ativo AND
                                                        pcv.chave::text = 'PeriodoMinimoAtendimento'::text
        ))) AND
        (l.codigodoserv_lanc IN (
                                  SELECT regexp_split_to_table(pcv.valor::text, ','::text)::integer AS regexp_split_to_table
                                  FROM centralassinante.parametrochavevalor pcv
                                       JOIN centralassinante.parametro p ON p.id = pcv.parametroid
                                  WHERE p.ativo AND
                                        pcv.chave::text = 'CodigosServicosListagemOrdemServico'::text
        ))
  UNION
  SELECT ('10000'::text || hg.id::text)::bigint AS id,
         c.cidade AS codcidade,
         c.id AS assinanteid,
         ct.id AS contratoid,
         hg.descricao AS servico,
         'CENTRAL DE ATENDIMENTO'::text AS nomeequipe,
         NULL::date AS dataagendamento,
         hg.d_data + COALESCE(hg.t_hora, '00:00:00'::time without time zone) AS dataatendimento,
         hg.d_datafechamento + COALESCE(hg.t_horafechamento, '00:00:00'::time without time zone) AS dataconclusao,
         hg.id AS numos,
         CASE
           WHEN hg.d_datafechamento IS NULL THEN 1
           ELSE 3
         END AS situacao,
         hg.descricao AS observacao
  FROM historicogeral hg
       JOIN assuntohistorico ah ON hg.assunto = ah.codigoassunto AND hg.grupoassunto = ah.codigogrupo
       JOIN clientes c ON hg.codigocidade = c.cidade AND hg.assinante = c.codigocliente
       JOIN contratos ct ON hg.codigocidade = ct.cidade AND hg.codcontrato = ct.contrato
  WHERE hg.d_datacadastro >('now'::text::date -((
                                                  SELECT pcv.valor::integer AS valor
                                                  FROM centralassinante.parametrochavevalor pcv
                                                       JOIN centralassinante.parametro p ON p.id = pcv.parametroid
                                                  WHERE p.ativo AND
                                                        pcv.chave::text = 'PeriodoMinimoAtendimento'::text
        ))) AND
        (ah.codigoassunto IN (
                               SELECT regexp_split_to_table(pcv.valor::text, ','::text)::integer AS regexp_split_to_table
                               FROM centralassinante.parametrochavevalor pcv
                                    JOIN centralassinante.parametro p ON p.id = pcv.parametroid
                               WHERE p.ativo AND
                                     pcv.chave::text = 'CodigosAssuntoHistoricoPermitidos'::text
        )) AND
        NOT (hg.controle IN (
                              SELECT CASE
                                       WHEN gh.historicopai IS NULL THEN gh.controle
                                       ELSE gh.historicopai
                                     END AS numhistorico
                              FROM ordemservico os_1
                                   JOIN historicogeral gh ON gh.controle = os_1.numhistorico
        ));

ALTER VIEW centralassinante.vis_ordemservico_web_simplificada
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

--VIEW ESTOQUE SERIADOS CONEXÃO
CREATE VIEW regrasoperacao.vis_estoqueseriados (
    codproduto,
    descproduto,
    codarmazem,
    descarmazem,
    numeroidentificacao,
    d_datatransferencia
)
AS
SELECT p.codigo,
       translate(p.descricao::text, ';'::text, ','::text) AS descricao,
       a.codigo AS codarmazem,
       a.descricao AS descarmazem,
       translate(mc.numeroidentificacao::text, ';'::text, ','::text) AS numeroidentificacao,
       t.d_datatransferencia
FROM materiaiscontrolados mc
JOIN movimentacaoproduto m ON m.codigo = mc.codmovimentacaoproduto
LEFT JOIN transferenciaprodutos t ON t.codigo = m.codtransferencia
JOIN armazem a ON a.codigo = mc.codarmazem
JOIN produtos p ON p.codigo = mc.codproduto;

ALTER VIEW regrasoperacao.vis_estoqueseriados
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

--FUNÇÃO RELATÓRIO ESTOQUE SERIADOS CONEXÃO
CREATE OR REPLACE FUNCTION relatoriospersonalizados.func_rp_grl_estoque_dispositivos_seriados (
)
RETURNS TABLE (
  cod_produto integer,
  desc_produto text,
  cod_armazem integer,
  desc_armazem varchar,
  identificador text,
  data_transferencia date
) AS
$body$
    BEGIN
    	Create temporary table temp_rp_grl_estoque_dispositivos_seriados(
          COD_PRODUTO integer,
          PRODUTO text,
          COD_ARMAZEM integer,
          ARMAZEM varchar(50),
          IDENTIFICADOR text,
          DATA_TRANSFERENCIA date
		) On commit drop;
        
        insert into temp_rp_grl_estoque_dispositivos_seriados
          select * from regrasoperacao.vis_estoqueseriados;
           
        return query select * from temp_rp_grl_estoque_dispositivos_seriados;
           
    end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100 ROWS 1000;

ALTER FUNCTION relatoriospersonalizados.func_rp_grl_estoque_dispositivos_seriados ()
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

--RELATÓRIO MOVIMENTAÇÃO ARMAZÉM CONEXÃO
select t.ordemservico,
       t.datamovimentacao,
       t.tipomovimentacao,
       t.numeromovimentacao,
       t.codarmazemorigem,
       t.armazemorigem,
       t.codarmazemdestino,
       t.armazemdestino,
       t.codigoproduto,
       t.descricaoproduto,
       t.nomeservico,
       t.contrato,
       t.quantidade
from(
select case when os.id is not null then os.numos::bigint else os1.numos::bigint end as ordemservico,
mv.d_datacadastro::date as datamovimentacao,
case
  when mv.codigopedido is not null then 'Pedido'::text
  when mv.codigorequisicao is not null then 'Requisição - Entrada'::text
  when position('REQ'::text in mv.idorigem::text) > 0 then 'Requisição - Saída'::text
  when mv.idmateriaisos is not null then 'Materiais Utilizados - Entrada'::text
  when position('MTU'::text in mv.idorigem::text) > 0 then 'Materiais Utilizados - Saída'::text
  when mv.idmateriaisretirados is not null then 'Materiais Retirados - Entrada'::text
  when position('MTR'::text in mv.idorigem::text) > 0 then 'Materiais Retirados - Saída'::text
  when mv.codtransferencia is not null then 'Transferência - Entrada'::text
  when position('TRF'::text in mv.idorigem::text) > 0 then 'Transferência - Saída'::text
  when mv.coddevolucao is not null then 'Devolução - Entrada'::text
  when position('DEV'::text in mv.idorigem::text) > 0 then 'Devolução - Saída'::text
end as tipomovimentacao,
mv.codigo::bigint as numeromovimentacao,
case when mv1.id::integer is not null then a1.id::integer else a2.id::integer end as codarmazemorigem,
case when mv1.id is not null then a1.descricao::text else a2.descricao::text end as armazemorigem,
a.codigo::integer as codarmazemdestino,
a.descricao::text as armazemdestino,
mv.codigoproduto::bigint as codigoproduto,
prd.descricao::text as descricaoproduto,
case when l.id is not null then l.descricaodoserv_lanc::text else l1.descricaodoserv_lanc::text end as nomeservico,
case when os.id is not null then os.codigocontrato::bigint else os1.codigocontrato::bigint end as contrato,
mv.quantidade::integer as quantidade
from public.movimentacaoproduto mv
join public.produtos prd on prd.codigo = mv.codigoproduto
join public.armazem a on a.codigo = mv.codarmazem
left join public.movimentacaoproduto mv1 on mv1.id = btrim(substr(mv.idorigem,4,20))::bigint
left join public.movimentacaoproduto mv2 on mv.id = btrim(substr(mv2.idorigem,4,20))::bigint
left join public.armazem a1 on a1.codigo = mv1.codarmazem   
left join public.armazem a2 on a2.codigo = mv2.codarmazem   
left join public.materiaisos mtu on mtu.id = mv.idmateriaisos
left join public.materiaisosretirada mtr on mtr.id = mv.idmateriaisretirados
left join public.ordemservico os on os.cidade = mtu.codigocidade and os.codempresa = mtu.codempresa and os.numos = mtu.numos
left join public.ordemservico os1 on os1.cidade = mtr.codigocidade and os1.codempresa = mtr.codempresa and os1.numos = mtr.numos
left join public.lanceservicos l on l.codigodoserv_lanc = os.codservsolicitado
left join public.lanceservicos l1 on l1.codigodoserv_lanc = os1.codservsolicitado
) as t
where t.datamovimentacao = '20220901' and t.codarmazemorigem = 90

--------------------------------------------------------------------------------------------------------------------------------------------

--FUNÇÃO FILTRO CONEXÃO
CREATE OR REPLACE FUNCTION relatoriospersonalizados.func_rp_estoque_movimentacao_produtos (
  pdatainicial date,
  pdatafinal date,
  parmorigem integer,
  parmdestino integer
)
RETURNS TABLE (
  ordemservico bigint,
  datamovimentacao date,
  tipomovimentacao text,
  numeromiventacao bigint,
  codarmazemorigem integer,
  armazemorigem text,
  codarmazemdestino integer,
  armazemdestino text,
  codigoproduto bigint,
  descricaoproduto text,
  nomeservico text,
  contrato bigint,
  quantidade integer
) AS
$body$
begin
  return query
	select * from relatoriospersonalizados.vis_estoque_movimentacao_produtos_armazem v
  where v.datamovimentacao between pDataInicial and pDataFinal and 
        v.codarmazemorigem = pArmOrigem and v.codarmazemdestino = pArmDestino
  order by v.ordemservico;
end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100 ROWS 1000;

ALTER FUNCTION relatoriospersonalizados.func_rp_estoque_movimentacao_produtos (pdatainicial date, pdatafinal date, parmorigem integer, parmdestino integer)
  OWNER TO postgres;

--------------------------------------------------------------------------------------------------------------------------------------------

-- TIRAR ENTER SQL
translate(os.observacoes, E'[\r\n]+', ' ')

--------------------------------------------------------------------------------------------------------------------------------------------

-- PROCURA CONTEÚDO EM FUNÇÕES
create temporary table funcoes as
  select n.nspname as schema_name,
         p.proname as function_name,
         pg_get_functiondef(p.oid) AS func_def,
         pg_get_function_arguments(p.oid) AS args,
         pg_get_function_result(p.oid) AS result
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname in ('public','idhcp','radius','syslog','provisionamento','temporarias','centralassinante','central','dpds','gerencial','integracao','ipay',
    'ipay_imanager','migracao','mobile','mysql','procedimentosbancarios','regrasoperacao','simulafaturamento','tmp_ins','totvs')    
  order by n.nspname, p.proname;

select f.*
from funcoes f
where f.func_def iLike '%rFatura.numero%'

--------------------------------------------------------------------------------------------------------------------------------------------

-- Assinatura qe não permite editar verificar o que falta
select * from interfocusprospect.assinaturapacoteterceiros a 
where a.assinatura = 99181         --tab preco 1251-- 99181-- 
 
select * from interfocusprospect.vis_pacotetabela p where p.vis_id_tabela_preco = 1251

--------------------------------------------------------------------------------------------------------------------------------------------

CREATE VIEW relatoriospersonalizados.vis_geral_historicos_gerais_v3 (
    contrato,
    cidade,
    codigoassi,
    nome,
    cpf_cnpj,
    tipo_historico,
    protocolo,
    historico_pai,
    atendente,
    grupo_atendente,
    data_cadastro,
    hora_cadastro,
    data_fechamento,
    hora_fechamento,
    tempo_atendimento,
    grupo,
    contato,
    assunto,
    telefone,
    situacaocontrato,
    status,
    razao_social,
    carteira,
    id_contrato,
    situacaoassunto)
AS
 WITH s AS (
SELECT hg_1.id,
                CASE
                    WHEN hg_1.d_datafechamento IS NOT NULL THEN 1
                    WHEN hg_1.d_datafechamento IS NULL AND hpai_1.d_datafechamento IS NOT NULL THEN 1
                    ELSE 2
                END AS status
FROM historicogeral hg_1
             LEFT JOIN historicogeral hpai_1 ON hpai_1.controle = hg_1.historicopai
        )
    SELECT DISTINCT ct.contrato,
    ci.nomedacidade AS cidade,
    cli.codigocliente AS codigoassi,
    cli.nome,
    cli.cpf_cnpj,
        CASE
            WHEN hg.historicopai IS NULL THEN 'Principal'::text
            ELSE 'Andamento'::text
        END AS tipo_historico,
    hg.controle AS protocolo,
    hg.historicopai AS historico_pai,
    hg.atendente,
    hga.namegroup AS grupo_atendente,
    hg.d_datacadastro AS data_cadastro,
    hg.t_horacadastro AS hora_cadastro,
    hg.d_datafechamento AS data_fechamento,
    hg.t_horafechamento AS hora_fechamento,
        CASE
            WHEN hg.d_datafechamento IS NOT NULL THEN (((hg.d_datafechamento || ' '::text) || hg.t_horafechamento)::timestamp without time zone) -
                (((hg.d_data || ' '::text) || hg.t_hora)::timestamp without time zone)
            WHEN hg.d_datafechamento IS NULL AND hpai.d_datafechamento IS NOT NULL THEN (((hpai.d_datafechamento || ' '::text) ||
                hpai.t_horafechamento)::timestamp without time zone) - (((hpai.d_data || ' '::text) || hpai.t_hora)::timestamp without time zone)
            ELSE NULL::interval
        END AS tempo_atendimento,
    translate(g.descricao::text, '.-;:,'::text, ','::text) AS grupo,
    translate(hg.descricao, E'[\r\n]+;:,', ' ') AS contato,
    translate(a.descricao::text, '.-:;,'::text, ','::text) AS assunto,
    func_retornatelefones(ct.cidade, ct.codigodocliente) AS telefone,
    v.descricaosituacao AS situacaocontrato,
        CASE
            WHEN s.status = 1 THEN 'fechado'::text
            ELSE 'aberto'::text
        END AS status,
    e.razaosocial AS razao_social,
    ca.descricao AS carteira,
    ct.id AS id_contrato,
    t.descricao AS situacaoassunto
    FROM historicogeral hg
     JOIN contratos ct ON ct.cidade = hg.codigocidade AND ct.codempresa = hg.codempresa AND ct.contrato = hg.codcontrato
     JOIN clientes cli ON cli.cidade = ct.cidade AND cli.codigocliente = ct.codigodocliente
     JOIN cidade ci ON ci.codigodacidade = ct.cidade
     JOIN empresas e ON e.codcidade = ct.cidade AND e.codempresa = ct.codempresa
     LEFT JOIN historicogeral hpai ON hpai.controle = hg.historicopai
     JOIN assuntohistorico a ON a.codigogrupo = hg.grupoassunto AND a.codigoassunto = hg.assunto
     JOIN grupohistorico g ON g.codigo = hg.grupoassunto
     LEFT JOIN usuariosdohistorico u ON u.controlehistorico = hg.controle
     LEFT JOIN hwusers hu ON lower(hu.login::text) = lower(u.usuario::text)
     LEFT JOIN hwgroups hgr ON hgr.id = hu.groupid
     LEFT JOIN hwusers hua ON lower(hua.login::text) = lower(hg.atendente::text)
     LEFT JOIN hwgroups hga ON hga.id = hua.groupid
     LEFT JOIN tiposituacaohistorico t ON t.codigo = hg.codigotiposituacao
     JOIN vis_situacaocontrato_descricao v ON v.situacao = ct.situacao
     JOIN carteira ca ON ca.codigo = ct.codcarteira
     JOIN s ON s.id = hg.id;

ALTER VIEW relatoriospersonalizados.vis_geral_historicos_gerais_v3
  OWNER TO postgres;


--

CREATE OR REPLACE FUNCTION relatoriospersonalizados.func_rp_grl_historicos_gerais_v3 (
  pdatainicial date,
  pdatafinal date,
  phistorico text
)
RETURNS TABLE (
  "STATUS" text,
  "TIPO_HISTORICO" text,
  "HISTORICO_PAI" integer,
  "ASSUNTO" varchar,
  "CARTEIRA" varchar,
  "DATA_CADASTRO" date,
  "HORA_CADASTRO" time,
  "CPF/CNPJ" varchar,
  "NOME" varchar,
  "CONTRATO" integer,
  "ID_CONTRATO" integer,
  "PROTOCOLO" text,
  "ATENDENTE" varchar,
  "GRUPO" varchar,
  "CONTATO" varchar,
  "DATA_FECHAMENTO" date,
  "HORA_FECHAMENTO" time,
  "TEMPO_ATENDIMENTO" time,
  "CIDADE" varchar,
  "TELEFONE" text,
  "SITUAÇÃO_CONTRATO" text,
  "SITUAÇÃO_ASSUNTO" varchar,
  "CÓDIGO_ASSI" integer,
  "RAZAO_SOCIAL" varchar
) AS
$body$
      BEGIN
    	Create temporary table temp_rp_grl_historicos_gerais_v3(
           "STATUS" text,
            "TIPO_HISTORICO" text,
            "HISTORICO_PAI" integer,
            "ASSUNTO" varchar(100),
            "CARTEIRA" varchar(100),
            "DATA_CADASTRO" date,
            "HORA_CADASTRO" time,
            "CPF/CNPJ" varchar(18),
            "NOME" varchar (40),
            "CONTRATO" integer,
            "ID_CONTRATO" integer,
            "PROTOCOLO" text,
            "ATENDENTE" varchar(20),            
            "GRUPO" varchar(30),
            "CONTATO" varchar(23000),
            "DATA_FECHAMENTO" date,
            "HORA_FECHAMENTO" time,
            "TEMPO_ATENDIMENTO" time,
            "CIDADE" varchar(30),
            "TELEFONE" text,
            "SITUAÇÃO_CONTRATO" text,
            "SITUAÇÃO_ASSUNTO" varchar(100),
            "CÓDIGO_ASSI" integer,
            "RAZAO_SOCIAL" varchar(100)
		) On commit drop;
       
      		phistorico := lower(to_ascii(phistorico::text)) || '%';
      
        insert into temp_rp_grl_historicos_gerais_v3
        	select hg.status,
            hg.tipo_historico,
            hg.historico_pai,
            hg.assunto,
            hg.carteira,
            hg.data_cadastro,
            hg.hora_cadastro,
            hg.cpf_cnpj,
            hg.nome,
            hg.contrato,
            hg.id_contrato,
            hg.protocolo,
            hg.atendente,
            hg.grupo,
            hg.contato,
            hg.data_fechamento,
            hg.hora_fechamento,
            hg.tempo_atendimento,
            hg.cidade,
            hg.telefone,
            hg.situacaocontrato,
            hg.situacaoassunto,
            hg.codigoassi,
            hg.razao_social
            from relatoriospersonalizados.vis_geral_historicos_gerais_v3 hg
        where hg.data_cadastro BETWEEN pdatainicial and pdatafinal and 
        lower(to_ascii(hg.status::text)) ilike phistorico;
                             
        return query select * from temp_rp_grl_historicos_gerais_v3;
		                 
    end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100 ROWS 1000;

ALTER FUNCTION relatoriospersonalizados.func_rp_grl_historicos_gerais_v3 (pdatainicial date, pdatafinal date, phistorico text)
  OWNER TO postgres;
 
--------------------------------------------------------------------------------------------------------------------------------------------









