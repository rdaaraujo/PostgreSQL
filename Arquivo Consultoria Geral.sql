--*******************************************************************************************
--Base de Clientes 
--*******************************************************************************************
select distinct cli.cpf_cnpj as "CPF/CNPJ",
case when ct.formapagamento = 1 and ct.faturaimpressa = 1 then 'Boleto Bancário Digital'
     when ct.formapagamento = 1 and ct.faturaimpressa = 2 then 'Boleto Bancário Impresso'
     when ct.formapagamento = 2 then 'Deposito Identificado'
     when ct.formapagamento = 3 then 'Débito Automático'
     when ct.formapagamento = 4 then 'Cartão de Crédito'
End as "Tipo Documento", cli.nome as "Nome", cli.codigocliente as "Código", 
ttc.descricao as "Tipo Cliente",
'' as "Tipo Cliente 2",
cli.d_datanascimento as "Data Nascimento", 
p.descricao as "Profissão", 
cli.estadocivil as "Estado Civil",
case when cli.sexo = 1 then 'Masculino'
     when cli.sexo = 2 then 'Feminino'
     else 'Não Informado'
End as "Sexo",
case when cli.tipoassinante = 1 then 'Comercial/Industrial'
     when cli.tipoassinante = 2 then 'Poder Publico'
     when cli.tipoassinante = 3 then 'Residencial/Pessoa Fisica'
     when cli.tipoassinante = 4 then 'Publico'
     when cli.tipoassinante = 5 then 'Semi-Publico'
     when cli.tipoassinante = 6 then 'Outros'
end as "Segmento do cliente",  
v.descricaosituacao as "Status do Cliente", 
cd.dataposicao as "Data do Status do cliente",
ttc.descricao as "Classificação COmercial",
ci.nomedacidade as "Cidade", ci.codigodacidade as "Código Cidade",
ct.bairroconexao as "Bairro Conexão",
ct.cepconexao as "CEP Conexão",
e.tipodologradouro ||' '|| e.nomelogradouro ||  ', Nº'|| ct.numeroconexao AS "Local de Instalacao", ct.numeroconexao as "Nº Conexão",  
tc.nome as "Cidade Cobrança", tc.codigo as "Código Cidade Cobrança",
ct.bairrocobranca as "Bairro Cobrança", 
ct.cepcobranca as "CEP Cobrança",
ec.tipodologradouro ||' '|| ec.nomelogradouro ||  ', Nº'|| ct.numerocobranca AS "Local de Cobrança", ct.numerocobranca as "Nº Cobrança", 
ct.id as "Código da Conexão",
'' "Qtde Planos Contratos",
(  select count(*)
   from gerencial.pacotesdiarios cp
   where cp.idcontrato=ct.id and cp.dataposicao=cd.dataposicao
) as "Qtde Produtos Contratos",
ct.d_datadavenda as "Data Venda",
ct.d_datadainstalacao as "Data Instalação",
ct.d_datadainstalacao as "Data Inicio do Relacionamento",
current_date - ct.d_datadainstalacao as "Tempo de Relacionamento (dias)",
pd.idpacote as "Código do Produto",
pd.valorpacote as "Valor do Produto",
cd.dataposicao as "Data do Status do cliente",
aa.codaditivo as "Código Campanha Adquirida",
aa.descricao as "Nome Campanha Adquirida",
case when aa.codaditivo is not null then ad.d_datainicio else null end as "Inicio Campanha",
case when aa.codaditivo is not null then ad.d_datafim else null end as "Fim Campanha",
aa.valordesconto as "Desconto Campanha",
t.id as "id Tabela Preço",
pd.idpacote as "ID Produto",
pd.nomepacote as "Nome Produto",
pd.valorpacote as "Valor Produto" ,
cca.descricao as "Empresa"
/*array_to_string(ARRAY(
   select cp.idpacote
   from gerencial.pacotesdiarios cp
   where cp.idcontrato=ct.id and cp.dataposicao=cd.dataposicao
), ',', '') as "ID Produto"*/
from contratos ct
join gerencial.contratosdiarios cd on cd.idcontrato=ct.id
join gerencial.pacotesdiarios pd on pd.idcontrato=cd.idcontrato and pd.dataposicao=cd.dataposicao
left join auditoria.aud_cont_prog cp1 on cp1.id = pd.idcontprog and cp1.tipoaud='I'
left join tabeladeprecos t on t.codcidade=cp1.cidade and t.codigo=cp1.codigodatabeladeprecos
join clientes cli on cli.cidade=ct.cidade and cli.codigocliente=ct.codigodocliente 
join cidade ci on ci.codigodacidade=ct.cidade 
join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
join enderecos ec on ec.codigodacidade=ct.cidadecobranca and ec.codigodologradouro=ct.enderecodecobranca
join tablocal tc on tc.codigo=ct.cidadecobranca
join vis_situacaocontrato_descricao v on v.situacao=ct.situacao
join tiposcontrato ttc on ttc.codigo=ct.tipodocontrato
left join profissoes p on p.codigo=cli.profissao
left join aditivoscontratos ad on ad.codcidade=ct.cidade and ad.codempresa=ct.codempresa and ad.numcontrato=ct.contrato
left join aditivos aa on aa.codaditivo=ad.codaditivo and aa.valordesconto > 0
join carteira cca on cca.codigo=ct.codcarteira
where /*cli.codigocliente between 312591 and 613591 and ci.codigodacidade = 121491
and */cd.dataposicao between '2022-06-01' and '2022-06-30'

--*******************************************************************************************
--Tabela de Preço
--*******************************************************************************************
WITH 
  dest as (
    select t.id, min(t.dataaud::date) as desativacao 
    from auditoria.aud_tabeladeprecos t 
    where t.tabelaativa = 0 
    group by t.id
  ),
  ativ as (
    select t.id, min(t.dataaud::date) as ativacao 
    from auditoria.aud_tabeladeprecos t 
    where t.tabelaativa = 1 
    group by t.id
  )
select cid.nomedacidade as "Cidade", t.id as "ID Tabela Preço", t.nomedatabeladeprecos as "Nome Tabela Preço", case when t.tabelaativa = 1 then 'Ativa' else 'Inativa' end as status,
case when a.ativacao is not null then a.ativacao else t.d_datacadastro end as "Data Ativação", 
case when t.tabelaativa = 1 then null else d.desativacao end as "Data Desativacao",
pp.id as "ID Produto",
pp.nomedaprogramacao as "Nome Produto",
p.valordaprogramacao as "Valor Produto",
case when pp.liberadaparavenda = 1 then 'Ativo' else 'Inativo' end as "Produto Status"
from tabeladeprecos t
join cidade cid on cid.codigodacidade=t.codcidade
join prodtabelapreco p on p.codcidade=t.codcidade and p.codigodatabela=t.codigo
join programacao pp on pp.codcidade=p.codcidade and pp.codigodaprogramacao=p.codigodaprogramacao
left join dest d on d.id=t.id
left join ativ a on a.id=t.id


--*******************************************************************************************
--Faturamento
--*******************************************************************************************
select distinct cli.cpf_cnpj as "CPF/CNPJ", 
cli.nome as "Nome", 
cli.codigocliente as "Código", 
dr.numerodocumento as "Nº Documento",
case when dr.formadepagamento = 1 then 'Boleto'
     when dr.formadepagamento = 2 then 'Deposito'
     when dr.formadepagamento = 3 then 'Debito Automático'
     when dr.formadepagamento = 4 then 'Cartao de Credito'
end as "Tipo Documento",
ci.nomedacidade as "Cidade",
e.tipodologradouro ||' '|| e.nomelogradouro ||  ', Nº'|| ct.numeroconexao AS "Local de Instalacao", ct.numeroconexao as "Nº Conexão", ct.bairroconexao as "Bairro Conexão",
ec.tipodologradouro ||' '|| ec.nomelogradouro ||  ', Nº'|| ct.numerocobranca AS "Local de Cobrança", ct.numerocobranca as "Nº Cobrança", ct.bairrocobranca as "Bairro Cobrança",
ct.id as "Código Conexão",
case when mf.lanc_servico is not null and mf.numerodaprogramacao is not null then 'Pacote/Serviços'
     when mf.lanc_servico is null and mf.numerodaprogramacao is not null then 'Pacote'
     when mf.lanc_servico is not null and mf.numerodaprogramacao is null then 'Serviços'
     else 'Adesão'
end as "Produto/Serviço",
case when mf.lanc_servico is not null then l.descricaodoserv_lanc
     when mf.numerodaprogramacao is not null then pr.nomedaprogramacao
     else 'Adesão'
end as "Tipo Produto/Serviço",
case when mf.lanc_servico is not null then l.id else pr.id end as "Código Produto",
pr.id as "Código do Plano",
dr.nossonumero as "Código Fatura",
(
  select max(nf.d_dataemissao)
  from nfviaunica nf  
  where nf.idboleto=dr.id
) as  "Data Emissão",
dr.d_datapagamento as "Data Pagamento",
dr.d_datafaturamento as "Data Faturamento",
dr.d_datavencimento as "Data Vencimento", 
m.descmotivo as "Motivo Cancelamento",
case when dr.situacao = 0 then 'Normal' else 'Cancelado' end as "Situação Documento", 
case when dr.tipopagamento = 1 then 'Dinheiro'
     when dr.formadepagamento = 2 then 'Cheque'
     when dr.formadepagamento = 3 then 'Banco'
     when dr.formadepagamento = 4 then 'Cartao de Credito'
     when dr.formadepagamento = 4 then 'Cartao de Débito'
end as "Tipo Pagamento",
case when dr.tipoboleto = 1 then 'Fatuaramento Mensal'
     when dr.tipoboleto = 2 then 'Faturamento Parcial'
     when dr.tipoboleto = 3 then 'Faturamento Individual'
     when dr.tipoboleto = 4 then 'Faturamento Avulso'
     when dr.tipoboleto = 5 then 'Fatuaramento Mensam Individual'
     else 'Não Informado'
End as "Tipo Faturamento",
dr.valordesconto as "Valor Desconto",
mf.valoros as "Valor Documento",
dr.valorjuros as "Valor Juros",
dr.valormulta as "Valor Multa",
case when dr.d_datapagamento is not null then mf.valoros else 0 end as "Valor Pago",
case when dr.reparcelamento = 1 then 'Reparcelamento' 
     when dr.boletoequipamento = 1 then 'Multa Fidelidade/Equipamento'
     else 'Mensalidade' 
end as "Boleto Reparcelado",
cca.descricao as "Empresa"
from docreceber dr
join movimfinanceiro mf on mf.numfatura = dr.fatura 
join clientes cli on cli.codigocliente = dr.cliente and cli.cidade = dr.codigodacidade
join contratos ct on ct.cidade=mf.cidade and ct.codempresa=mf.codempresa and ct.contrato=mf.contrato
join carteira cca on cca.codigo=ct.codcarteira
join cidade ci on ci.codigodacidade = cli.cidade
join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
join enderecos ec on ec.codigodacidade=ct.cidadecobranca and ec.codigodologradouro=ct.enderecodecobranca
left join programacao pr on pr.codcidade=mf.cidade and pr.codigodaprogramacao=mf.numerodaprogramacao
left join lanceservicos l on l.codigodoserv_lanc=mf.lanc_servico
left join motivocancelamento m on m.codmotivo=dr.motivocancelamento
where /*cli.codigocliente between 312591 and 613591 and ci.codigodacidade = 121491
and */dr.d_datafaturamento between '2022-05-01' and '2022-05-31'





--*******************************************************************************************
--Faturamento - NF - VERSAO 01
--*******************************************************************************************
select x."Empresa", x."Cidade", x."Código Cliente", x."Nome", x."CPF/CNPJ", 
max(x."Tipo Faturamento") as tipo_faturamento,
max(x."ID Conexão") as id_contrato, 
max(x."Tipo Contrato") as tipo_contrato,
max(x."Local de Instalacao") as "Local de Instalacao",
max(x."Nº Conexão") as "Nº Conexão",
max(x."Bairro Conexão") as "Bairro Conexão",
max(x."Local de Cobrança") as "Local de Cobrança",
max(x."Nº Cobrança") as "Nº Cobrança",
max(x."Bairro Cobrança") as "Bairro Cobrança",
x."Descrição Movimento", x."Nº Documento", 
x."Data Vencimento", x."Valor Documento", 
x."CFOP", x."Data Emissão NF",  x."Data Emissão Boleto",
x."Nº NF", x."Serie NF", x."Tipo NF",
x."Modelo NF", x."Descrição Lançamento", x."Sistema", x."Status", x."Código Classificação", 
x."Descrição Classificação", 
x."Valor Item",
x."Base ICMS",
x."Aliquota Redução ICMS", 
x."Valor ICMS", 
x."Aliquota ICMS",
x."Valor PIS",
x."Aliquita PIS",
x."Valor COFINS",
x."Aliquota COFINS",
x."Valor FUST", 
x."Aliquota FUST",
x."Valor Funtel", 
x."Aliquota Funtel",
x."Valor ISS",
x."Aliquota ISS"
from (
       SELECT DISTINCT 
         u.descricao as "Empresa",
         cid.nomedacidade as "Cidade",
         cli.codigocliente as "Código Cliente",
         cli.nome as "Nome",
         cli.cpf_cnpj as "CPF/CNPJ",
         CASE WHEN length(translate(cli.cpf_cnpj::text, '.,//- '::text, ''::text))= 14 THEN 'PJ'::text ELSE 'PF'::text END AS "Tipo Assinante",
         CASE WHEN ct.gerarcobranca = 0 then 'Acumulado por empresa'
              WHEN ct.gerarcobranca = 1 then  'Somente do contrato'
              WHEN ct.gerarcobranca = 2 then  'Acumulado por cliente'
         END AS "Tipo Faturamento",
         ct.id as "ID Conexão",
         e.tipodologradouro ||' '|| e.nomelogradouro ||  ', Nº'|| ct.numeroconexao AS "Local de Instalacao", ct.numeroconexao as "Nº Conexão", ct.bairroconexao as "Bairro Conexão",
         ec.tipodologradouro ||' '|| ec.nomelogradouro ||  ', Nº'|| ct.numerocobranca AS "Local de Cobrança", ct.numerocobranca as "Nº Cobrança", ct.bairrocobranca as "Bairro Cobrança",
         tc.descricao as "Tipo Contrato",
         ex.descricao as "Descrição Movimento",
         d.numerodocumento as "Nº Documento",
         d.d_datavencimento as "Data Vencimento",
         d.valordocumento as "Valor Documento",
         n.cfop as "CFOP",
         n.d_dataemissao as "Data Emissão NF",
         case when d.d_dataemissao is not null then d.d_dataemissao else d.d_datacadastro end as "Data Emissão Boleto",
         n.numnf as "Nº NF",
         n.serienf as "Serie NF",
         CASE WHEN n.tiponf = 1 THEN n.nf_modelo::text ELSE 'ND'::text END as "Tipo NF",
         CASE when i.idnfiss > 0 then 'Prefeitura'::text
              WHEN i.codclassificacaoconv115 IS NOT NULL THEN 'Telecom'::text
              ELSE 'Débito'::text
         END AS "Modelo NF",
         CASE WHEN p.id IS NOT NULL THEN p.nomedaprogramacao
              WHEN l.id IS NOT NULL THEN l.descricaodoserv_lanc
              ELSE i.descricao
         END as "Descrição Lançamento",
         CASE WHEN p.codprodutoextratificacao = ANY (ARRAY [ 21, 31 ]) THEN 'Internet'::text
              WHEN p.codprodutoextratificacao = 11 THEN 'Telefonia'::text
              WHEN p.id IS NOT NULL THEN vpr.descricaotipoprogramacao
              WHEN l.codextratificacao = 51 THEN 'Internet'::text
              WHEN l.codextratificacao = 61 THEN 'Telefonia'::text
              WHEN l.servicooulancamento = 1 THEN 'Serviços Técnicos'::text
              WHEN l.servicooulancamento = 2 THEN 'Lançamentos Financeiros'::text
              ELSE 'Diversos'::text
         END AS "Sistema",
         CASE WHEN n.d_datacancelamento IS NULL THEN 'ATIVA'::text ELSE 'CANCELADA'::text END "Status",
         i.codclassificacaoconv115 "Código Classificação",
         cl.descricao as "Descrição Classificação",
         i.valoritem as "Valor Item",
         i.baseicms as "Base ICMS",
         i.aliquotareducaoicms as "Aliquota Redução ICMS", 
         i.valoricms as "Valor ICMS", 
         i.aliquotaicms as "Aliquota ICMS",
         i.valorpis as "Valor PIS",
         i.aliquotapis as "Aliquita PIS",
         i.valorcofins as "Valor COFINS",
         i.aliquotacofins as "Aliquota COFINS",
         i.valorfust as "Valor FUST", 
         i.aliquotafust as "Aliquota FUST",
         i.valorfuntel as "Valor Funtel", 
         i.aliquotafuntel as "Aliquota Funtel",
         i.valoriss as "Valor ISS",
         i.aliquotaiss as "Aliquota ISS"
  FROM itensnf i
  JOIN nfviaunica n ON n.id::double precision = i.idnfconvenio
  JOIN fatura f ON f.numerofatura = i.numfatura
  JOIN movimfinanceiro m on m.numfatura = f.numerofatura
  JOIN contratos ct on ct.cidade = m.cidade and ct.codempresa = m.codempresa and ct.contrato = m.contrato
  join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
  join enderecos ec on ec.codigodacidade=ct.cidadecobranca and ec.codigodologradouro=ct.enderecodecobranca
  JOIN tiposcontrato tc on ct.tipodocontrato = tc.codigo
  JOIN docreceber d ON d.fatura = f.numerofatura
  JOIN cidade cid ON cid . codigodacidade = i.codcidade
  JOIN clientes cli ON cli.cidade = i.codcidade AND cli.codigocliente = f.codigodoassinante
  LEFT JOIN unificadora u ON u.codigo = i.codunificadora
  LEFT JOIN programacao p ON p.codigodaprogramacao = i.codpacote AND p.codcidade = i.codcidade
  LEFT JOIN vis_descritivo_tipo_programacao vpr ON vpr.tipoponto = p.tipoponto AND vpr.tipoprogramacao = p.tipoprogramacao
  LEFT JOIN lanceservicos l ON l.codigodoserv_lanc = i.codlancservico
  LEFT JOIN classificacao cl ON cl.codigo = i.codclassificacaoconv115
  LEFT JOIN estratificacao ex on p.codextratificacao =  ex.codigo
  where n.d_dataemissao between '2022-07-01' and '2022-07-31'
) as x
group by x."Empresa", x."Cidade", x."Código Cliente", x."Nome", x."CPF/CNPJ", x."Descrição Movimento", x."Nº Documento", x."Data Vencimento", x."Valor Documento", 
x."CFOP", x."Data Emissão NF",  x."Data Emissão Boleto", x."Nº NF", x."Serie NF", x."Tipo NF", x."Modelo NF", x."Descrição Lançamento", x."Sistema", x."Status", x."Código Classificação", 
x."Descrição Classificação", x."Valor Item",x."Base ICMS",x."Aliquota Redução ICMS", x."Valor ICMS", x."Aliquota ICMS",x."Valor PIS",x."Aliquita PIS",x."Valor COFINS",x."Aliquota COFINS",x."Valor FUST", 
x."Aliquota FUST",x."Valor Funtel", x."Aliquota Funtel",x."Valor ISS",x."Aliquota ISS"


--*******************************************************************************************
--Catálogo de Produtos
--*******************************************************************************************
select distinct cli.cpf_cnpj as "CPF/CNPJ",
cli.nome as "Nome", 
cli.codigocliente as "Código", 
ci.nomedacidade as "Cidade", 
ct.bairroconexao as "Bairro Conexão",
e.tipodologradouro ||' '|| e.nomelogradouro ||  ', Nº'|| ct.numeroconexao AS "Local de Instalacao", 
ct.numeroconexao as "Nº Conexão", 
p.nomedaprogramacao as "Produto", 
cp.valorpacote as "Preço do Produto", 
p.nomedaprogramacao as "Descrição Produto", 
dp.descricaotipoprogramacao as "Sistema do Produto",
tc.descricaotecnologia as "Tecnologia do Produto",
p.nomedaprogramacao as "Pacote", 
p.id as "Código Pacote",
p.id as "Código Produto",
dp.descricaotipoprogramacao as "Tipo do Plano",
p.nomedaprogramacao as "Descrição do Plano",
cp.id as "Plano Contratado",
tp.nomedatabeladeprecos as "Nome da Tabela de Preço",
cp.d_dataativacao as "Data de Criacao",
cp.d_datadesativacao as "Data de Termino", 
cp.d_dataativacao as "Liberada em",
cp.valorpacote as "Preço do plano",
cp.valorpacote as "Valor",
v.descricaosituacao as "Status Atual", 
ct.id as "Código da Conexão",
tp.id as "id_tabela_preco"
from cont_prog cp
join cidade ci on cp.cidade = ci.codigodacidade
join programacao p on p.codcidade = cp.cidade and p.codigodaprogramacao = cp.protabelaprecos
left join tipotecnologiapacote tc on tc.codtipotecnologia=p.codtipotecnologia
join tabeladeprecos tp on tp.codcidade = cp.cidade and tp.codigo = cp.codigodatabeladeprecos
join contratos ct on ct.cidade = cp.cidade and ct.codempresa=cp.codempresa and ct.contrato = cp.contrato
join clientes cli on cli.cidade = cp.cidade and ct.codigodocliente = cli.codigocliente
join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
join enderecos ec on ec.codigodacidade=ct.cidadecobranca and ec.codigodologradouro=ct.enderecodecobranca
join vis_situacaocontrato_descricao v on v.situacao=ct.situacao
join vis_descritivo_tipo_programacao dp on dp.tipoprogramacao=p.tipoprogramacao
--where cli.codigocliente between 312591 and 613591 and ci.codigodacidade = 121491


--*******************************************************************************************
--Catálogo de Produtos - TABELA DE PREÇO
--*******************************************************************************************
select distinct '' as "CPF/CNPJ",
'' as "Nome", 
'' as "Código", 
ci.nomedacidade as "Cidade", 
'' as "Bairro Conexão",
'' AS "Local de Instalacao", 
'' as "Nº Conexão", 
p.nomedaprogramacao as "Produto", 
cp.valordaprogramacao as "Preço do Produto", 
p.nomedaprogramacao as "Descrição Produto", 
dp.descricaotipoprogramacao as "Sistema do Produto",
tc.descricaotecnologia as "Tecnologia do Produto",
p.nomedaprogramacao as "Pacote", 
p.id as "Código Pacote",
p.id as "Código Produto",
dp.descricaotipoprogramacao as "Tipo do Plano",
p.nomedaprogramacao as "Descrição do Plano",
'' as "Plano Contratado",
tp.nomedatabeladeprecos as "Nome da Tabela de Preço",
tp.d_datacadastro as "Data de Criacao",
(select max(t.dataaud)::date from auditoria.aud_tabeladeprecos t where t.tabelaativa = 1 and t.id=tp.id)as "Data de Termino", 
tp.d_datacadastro as "Liberada em",
cp.valordaprogramacao as "Preço do plano",
cp.valordaprogramacao as "Valor",
case when tp.tabelaativa = 0 then 'Ativo' else 'Inativo' end as "Status Atual", 
'' as "id_contrato",
tp.id as "id_tabela_preco"
from prodtabelapreco cp
join cidade ci on cp.codcidade = ci.codigodacidade
join programacao p on p.codcidade = cp.codcidade and p.codigodaprogramacao = cp.codigodaprogramacao
left join tipotecnologiapacote tc on tc.codtipotecnologia=p.codtipotecnologia
join tabeladeprecos tp on tp.codcidade = cp.codcidade and tp.codigo = cp.codigodatabela
join vis_descritivo_tipo_programacao dp on dp.tipoprogramacao=p.tipoprogramacao
where /*ci.codigodacidade = 121491 and*/ to_ascii(tp.nomedatabeladeprecos) not ilike '%migracao%'


--*******************************************************************************************
--Vendas
--*******************************************************************************************
select distinct 
cli.cpf_cnpj as "CPF/CNPJ",
case when ct.formapagamento = 1 and ct.faturaimpressa = 1 then 'Boleto Bancário Digital'
     when ct.formapagamento = 1 and ct.faturaimpressa = 2 then 'Boleto Bancário Impresso'
     when ct.formapagamento = 2 then 'Deposito Identificado'
     when ct.formapagamento = 3 then 'Débito Automático'
     when ct.formapagamento = 4 then 'Cartão de Crédito'
End as "Tipo Documento",
cli.nome as "Nome", 
ct.bairrocobranca as "Bairro Cobrança",
cli.codigocliente as "Código", 
ci.nomedacidade as "Cidade", 
ct.bairroconexao as "Bairro Conexão",
case when  ct.formapagamento = 1 then 'Boleto'
     when  ct.formapagamento = 2 then 'Deposito'
     when  ct.formapagamento = 3 then 'Debito'
     when  ct.formapagamento = 4 then 'Cartao de Credito'
end as "Local de Cobrança",
tv.descricao as "Canal de contratação", 
e.tipodologradouro ||' '|| e.nomelogradouro ||  ', Nº'|| ct.numeroconexao AS "Local de Instalacao", ct.numeroconexao as "Nº Conexão", 
ec.tipodologradouro ||' '|| ec.nomelogradouro ||  ', Nº'|| ct.numerocobranca AS "Local de Cobrança", ct.numerocobranca as "Nº Cobrança", 
p.nomedaprogramacao as "Produto Contratado", 
cp.valorpacote as "Preco do Produto", 
ct.id as "Código Conexão",
'' as "Código Movimentação",
p.id as "Código do Plano",
ct.d_datadainstalacao as "Data Instalação",
ct.d_datadavenda as "Data Venda",
0 as "Descontos e Ofertas",
'' as "Margem do Produto",
ct.numeroconexao as "Nº Conexão",
p.nomedaprogramacao as "Plano Contratado",
cp.valorpacote as "Preço do Plano",
cp.valorpacote as "Preço do Produto",
p.nomedaprogramacao as "Produto Contratado",
'' as "Termos ou Condições Especiais",
case when  ct.formapagamento = 1 then 'Boleto'
     when  ct.formapagamento = 2 then 'Deposito'
     when  ct.formapagamento = 3 then 'Debito'
     when  ct.formapagamento = 4 then 'Cartao de Credito'
end as "Pagamento Selecionado",
dp.descricaotipoprogramacao as "Tipo do Plano", 'Cliente Novo',
t.id as "ID Tabela Preço"
from contratos ct
join clientes cli on cli.cidade=ct.cidade and cli.codigocliente=ct.codigodocliente 
join cidade ci on ci.codigodacidade=ct.cidade 
join cont_prog cp on cp.cidade=ct.cidade and cp.contrato=ct.contrato and cp.codempresa=ct.codempresa 
join tabeladeprecos t on t.codcidade=cp.cidade and t.codigo=cp.codigodatabeladeprecos
join programacao p on p.codigodaprogramacao=cp.protabelaprecos and p.codcidade=cp.cidade 
join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
join enderecos ec on ec.codigodacidade=ct.cidadecobranca and ec.codigodologradouro=ct.enderecodecobranca
left join tiposdevenda tv on tv.codigo=ct.tipodevenda
join vis_situacaocontrato_descricao v on v.situacao=ct.situacao
join vis_descritivo_tipo_programacao dp on dp.tipoprogramacao=p.tipoprogramacao
where ct.d_datadavenda between '2022-05-01' and '2022-09-30' 
--and  cli.codigocliente between 312591 and 613591 and cli.cidade = 121491
--Movimentações



--*******************************************************************************************
-- Dados Cadastrais 
--*******************************************************************************************
select distinct 
cli.cpf_cnpj as "CPF/CNPJ",
cli.nome as "Nome", 
cli.codigocliente as "Código", 
case when cli.sexo = 1 then 'Masculino'
     when cli.sexo = 2 then 'Feminino'
     else 'Não Informado' 
end as "Genero",
cli.d_datanascimento as "Idade",
pro.descricao as "Profissao",
'' as "Renda",
cli.estadocivil as "Estado Civil", 
'' as "Nível Escolaridade",
ci.nomedacidade as "Cidade", 
ct.bairroconexao as "Bairro Conexão",
ci.nomedacidade as "Cidade Instalação", 
e.tipodologradouro ||' '|| e.nomelogradouro ||  ', Nº'|| ct.numeroconexao AS "Local de Instalacao", ct.numeroconexao as "Nº Conexão",  ct.cepconexao as "CEP Conexão",
ct.d_datadainstalacao as "Data Instalação",
t.nome as "Cidade Cobrança", ec.tipodologradouro ||' '|| ec.nomelogradouro ||  ', Nº'|| ct.numerocobranca AS "Local de Cobrança", ct.numerocobranca as "Nº Cobrança", ct.bairrocobranca as "Bairro Cobrança", ct.cepcobranca as "CEP Cobranca",
ct.d_datadavenda as "Data Venda",
case when  ct.formapagamento = 1 then 'Boleto'
     when  ct.formapagamento = 2 then 'Deposito'
     when  ct.formapagamento = 3 then 'Debito'
     when  ct.formapagamento = 4 then 'Cartao de Credito'
end as "Forma de Pagamento Selecionado",
 v.descricaosituacao as "Status Atual", 
 ct.id as "Código Conexão",
 p.id as "ID Produto",
 p.nomedaprogramacao as "Nome Produto",
 cp.valorpacote as "Valor Produto",
 tt.id as "ID Tabela Preço",
 aa.codaditivo as "Código Campanha Adquirida",
 aa.descricao as "Nome Campanha Adquirida",
 case when aa.codaditivo is not null then ad.d_datainicio else null end as "Inicio Campanha",
 case when aa.codaditivo is not null then ad.d_datafim else null end as "Fim Campanha"
from contratos ct
join clientes cli on cli.cidade=ct.cidade and cli.codigocliente=ct.codigodocliente 
join cidade ci on ci.codigodacidade=ct.cidade 
join cont_prog cp on cp.cidade=ct.cidade and cp.contrato=ct.contrato and cp.codempresa=ct.codempresa 
join tabeladeprecos tt on tt.codcidade=cp.cidade and tt.codigo=cp.codigodatabeladeprecos
join programacao p on p.codigodaprogramacao=cp.protabelaprecos and p.codcidade=cp.cidade 
join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
join enderecos ec on ec.codigodacidade=ct.cidadecobranca and ec.codigodologradouro=ct.enderecodecobranca
join tablocal t on t.codigo=ct.cidadecobranca
join vis_situacaocontrato_descricao v on v.situacao=ct.situacao
left join aditivoscontratos ad on ad.codcidade=ct.cidade and ad.codempresa=ct.codempresa and ad.numcontrato=ct.contrato
left join aditivos aa on aa.codaditivo=ad.codaditivo and aa.valordesconto > 0
left join profissoes pro on pro.codigo=cli.profissao
where ct.situacao <> 1 --and  cli.codigocliente between 312591 and 613591 and ci.codigodacidade = 121491




--*******************************************************************************************
-- CLientes Cancelados
--*******************************************************************************************
with 
  canc as (
    select t.idcontrato, oo.d_dataexecucao, case when m.descmotivo is null then 'INVOLUNTÁRIO' ELSE 'VOLUNTÁRIO' END AS cancelamento
    from (
      select ct.id as idcontrato, max(o.id) as idos
      from ordemservico o
      join contratos ct on ct.cidade=o.cidade and ct.codempresa=o.codempresa and ct.contrato=o.codigocontrato
      join lanceservicos l on l.codigodoserv_lanc=o.codservsolicitado
      where l.situacaocontrato = 5 and o.d_dataexecucao between '2022-05-01' and '2022-09-30'
      group by ct.id
    ) as t
    join ordemservico oo on oo.id=t.idos
    left join motivocancelamento m on m.codmotivo=oo.motivocancelamento
)
select distinct 
cli.cpf_cnpj as "CPF/CNPJ",
case when ct.formapagamento = 1 and ct.faturaimpressa = 1 then 'Boleto Bancário Digital'
     when ct.formapagamento = 1 and ct.faturaimpressa = 2 then 'Boleto Bancário Impresso'
     when ct.formapagamento = 2 then 'Deposito Identificado'
     when ct.formapagamento = 3 then 'Débito Automático'
     when ct.formapagamento = 4 then 'Cartão de Crédito'
End as "Tipo Documento" ,
cli.nome as "Nome", 
pro.descricao as "Profissao",
cli.estadocivil as "Estado Civil", 
case when cli.sexo = 1 then 'Masculino'
     when cli.sexo = 2 then 'Feminino'
     else 'Não Informado' 
end as "Genero",
cli.codigocliente as "Código", 
case when cli.tipoassinante = 1 then 'Comercial/Industrial'
     when cli.tipoassinante = 2 then 'Poder Publico'
     when cli.tipoassinante = 3 then 'Residencial/Pessoa Fisica'
     when cli.tipoassinante = 4 then 'Publico'
     when cli.tipoassinante = 5 then 'Semi-Publico'
     when cli.tipoassinante = 6 then 'Outros'
end as "Segmento do cliente", 
cli.d_datanascimento as "Data Nascimento",
ttc.descricao as "Tipo do Cliente",
v.descricaosituacao as "Status Cliente", 
current_date as "Data Status", 
ci.nomedacidade as "Cidade", 
ct.bairroconexao as "Bairro Conexão",
ct.cepcobranca as "CEP Cobranca",
ct.cepconexao as "CEP Conexão",
ci.codigodacidade as "Código Cidade",
e.tipodologradouro ||' '|| e.nomelogradouro ||  ', Nº'|| ct.numeroconexao AS "Local de Instalacao", ct.numeroconexao as "Nº Conexão",  
ct.d_datadainstalacao as "Data Instalação",
t.nome as "Cidade Cobrança",
ec.tipodologradouro ||' '|| ec.nomelogradouro ||  ', Nº'|| ct.numerocobranca AS "Local de Cobrança", ct.numerocobranca as "Nº Cobrança", ct.bairrocobranca as "Bairro Cobrança",
(
  select count(*) from contratos ct1
  join cont_prog cp1 on cp1.cidade=ct.cidade and cp1.contrato=ct.contrato and cp1.codempresa=ct.codempresa 
  where ct1.id=ct.id
) as "Qtde Planos Contratos",
ct.d_datadainstalacao as "Data Inicio Relacionamento",
cc.d_dataexecucao - ct.d_datadainstalacao as "Tempo Relacionamento",
ttc.descricao as "Classificação Comercial",
ct.id as "Código Conexão",
p.id as "Código Produto",
p.nomedaprogramacao as "Nome Produto",
'' as "Código Novo Produto",
cp.valorpacote as "Valor do Plano",
'' as "Código do Plano Anterior",
cc.d_dataexecucao as "Data Movimentaçaõ Cadastro",
'' as "Movimentação Produto",
'' as "Código Movimento Plano",
'' as "Data Execuçaõ Movimentação",
'' as "Data Solicitação Movimentação",
cc.d_dataexecucao as "Data Solicitação Cancelamento",
cc.d_dataexecucao as "Data Cancelamento",
(
  select count(*) from contratos ct1
  join cont_prog cp1 on cp1.cidade=ct.cidade and cp1.contrato=ct.contrato and cp1.codempresa=ct.codempresa 
  where ct1.id=ct.id
) as "Qtde Planos Cancelados" ,
cc.d_dataexecucao as "Data Desistalação",
tt.id as "ID Tabela Preço"
from contratos ct
join clientes cli on cli.cidade=ct.cidade and cli.codigocliente=ct.codigodocliente 
join cidade ci on ci.codigodacidade=ct.cidade 
join cont_prog cp on cp.cidade=ct.cidade and cp.contrato=ct.contrato and cp.codempresa=ct.codempresa 
join tabeladeprecos tt on tt.codcidade=cp.cidade and tt.codigo=cp.codigodatabeladeprecos
join programacao p on p.codigodaprogramacao=cp.protabelaprecos and p.codcidade=cp.cidade 
join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
join enderecos ec on ec.codigodacidade=ct.cidadecobranca and ec.codigodologradouro=ct.enderecodecobranca
join tablocal t on t.codigo=ct.cidadecobranca
join vis_situacaocontrato_descricao v on v.situacao=ct.situacao
join tiposcontrato ttc on ttc.codigo=ct.tipodocontrato
left join profissoes pro on pro.codigo=cli.profissao
join canc cc on cc.idcontrato=ct.id
where ct.situacao = 5 --limit 10000




--*******************************************************************************************
-- Histórico de Reajuste
--*******************************************************************************************
select cli.cpf_cnpj as "CPF/CNPJ",
cli.nome as "Nome Cliente",
case when cli.tipoassinante = 1 then 'Comercial/Industrial'
     when cli.tipoassinante = 2 then 'Poder Publico'
     when cli.tipoassinante = 3 then 'Residencial/Pessoa Fisica'
     when cli.tipoassinante = 4 then 'Publico'
     when cli.tipoassinante = 5 then 'Semi-Publico'
     when cli.tipoassinante = 6 then 'Outros'
end as "Segmento do cliente", 
v.descricaosituacao as "Status Clientes",
case when ct.formapagamento = 1 and ct.faturaimpressa = 1 then 'Boleto Bancário Digital'
     when ct.formapagamento = 1 and ct.faturaimpressa = 2 then 'Boleto Bancário Impresso'
     when ct.formapagamento = 2 then 'Deposito Identificado'
     when ct.formapagamento = 3 then 'Débito Automático'
     when ct.formapagamento = 4 then 'Cartão de Crédito'
End as "Tipo Documento",
ttc.descricao as "Tipo Cliente",
'' as "Tipo Cliente2",
cli.codigocliente as "Código Cliente",
ct.cepcobranca as "CEP Cobranca",
ct.cepconexao as "CEP Conexão",
ci.nomedacidade as "Cidade",
tl.nome as "Cidade Cobrança",
ttc.descricao as "Classificação Comercial",
ci.codigodacidade as "Código da Cidade",
ci.codigodacidade as "Código da Conexão",
'' as "Código Movimentação Plano",
'' as "Código Novo Plano",
r.codigodaprogramacao as "Código Plano Anterior",
r.codigodaprogramacao as "Código Produto",
r.d_data as "Data Execução Movimentação",
r.d_data as "Data Solicitação Movimentação",
ct.d_datadainstalacao as "Data Instalação",
cli.d_datanascimento as "Data Nacimento",
r.d_data as "Data Reajuste",
v.descricaosituacao as "Status Clientes",
ec.tipodologradouro ||' '|| ec.nomelogradouro ||  ', Nº'|| ct.numerocobranca AS "Local de Cobrança", ct.numerocobranca as "Nº Cobrança", ct.bairrocobranca as "Bairro Cobrança",
e.tipodologradouro ||' '|| e.nomelogradouro ||  ', Nº'|| ct.numeroconexao AS "Local de Instalacao",   
'' as "Movimentação Cadastro",
'' as "Movimentação Produto",
ct.numeroconexao as "Nº Conexão",
SUBSTRING(r.descricaoreajuste,38,6) as "Percentual Reajuste",
'' as "Quantidade de Planos Contratados",
'' as "Quantidade de Produtos Contratados",
current_date - ct.d_datadainstalacao  as "Tempo de Relacionamento",
r.valoratualpacote as "Valor Plano Anterior",
r.valorpacotereajustado as "Valor Plano Reajustado",
aa.codaditivo as "Código Campanha Adquirida",
aa.descricao as "Nome Campanha Adquirida",
case when aa.codaditivo is not null then ad.d_datainicio else null end as "Inicio Campanha",
case when aa.codaditivo is not null then ad.d_datafim else null end as "Fim Campanha",
aa.valordesconto as "Desconto Campanha",
tt.id as "ID Tabela Preço"
from reajustesefetivados r
join contratos ct on ct.id=r.contrato_id
left join cont_prog cp on cp.id=r.cont_prog_id
join tabeladeprecos tt on tt.codcidade=cp.cidade and tt.codigo=cp.codigodatabeladeprecos
left join programacao p on p.codcidade=cp.cidade and p.codigodaprogramacao=cp.protabelaprecos
join clientes cli on cli.cidade=ct.cidade and cli.codigocliente=ct.codigodocliente 
join cidade ci on ci.codigodacidade=ct.cidade 
join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
join enderecos ec on ec.codigodacidade=ct.cidadecobranca and ec.codigodologradouro=ct.enderecodecobranca
join tablocal tl on tl.codigo=ct.cidadecobranca
join vis_situacaocontrato_descricao v on v.situacao=ct.situacao
join tiposcontrato ttc on ttc.codigo=ct.tipodocontrato
left join aditivoscontratos ad on ad.codcidade=ct.cidade and ad.codempresa=ct.codempresa and ad.numcontrato=ct.contrato
left join aditivos aa on aa.codaditivo=ad.codaditivo and aa.valordesconto > 0
where r.d_data between '2022-05-01' and '2022-05-31' --limit 10000




--*******************************************************************************************
-- Contas Digitais
--*******************************************************************************************
with 
  email as (
    select ct.id, to_char(c.d_datacadastro,'YYYY-MM-01')::date as mes, count(*)
    from controlereimpressoes c
    join docreceber dr on dr.nossonumero=c.nossonumero
    join movimfinanceiro m on m.numfatura=dr.fatura
    join contratos ct on ct.cidade=m.cidade and ct.codempresa=m.codempresa and ct.contrato=m.contrato
    where c.tiporeimpressao = 1 and c.d_datacadastro between '2022-09-01' and '2022-09-30' and c.nomearquivo = 'ReguaCobranca'
    group by ct.id, mes
)
select cli.cpf_cnpj as "CPF/CNPJ",
case when ct.formapagamento = 1 and ct.faturaimpressa = 1 then 'Boleto Bancário Digital'
     when ct.formapagamento = 1 and ct.faturaimpressa = 2 then 'Boleto Bancário Impresso'
     when ct.formapagamento = 2 then 'Deposito Identificado'
     when ct.formapagamento = 3 then 'Débito Automático'
     when ct.formapagamento = 4 then 'Cartão de Crédito'
End as "Tipo Documento",
cli.nome as "Nome Cliente",
ct.bairrocobranca as "Bairro Cobrança",
cli.codigocliente as "Código Cliente",
ci.nomedacidade as "Cidade",
ct.bairroconexao as "Bairro Conexão",
case when  ct.formapagamento = 1 then 'Boleto'
     when  ct.formapagamento = 2 then 'Deposito'
     when  ct.formapagamento = 3 then 'Debito'
     when  ct.formapagamento = 4 then 'Cartao de Credito'
end as "Local Cobrança",
tv.descricao as "Canal de Contratação",
e.tipodologradouro ||' '|| e.nomelogradouro ||  ', Nº'|| ct.numeroconexao AS "Local de Instalacao",
ct.id as "Código Conexão",
p.id as "Código Plano",
p.nomedaprogramacao "Nome Plano",
ct.numeroconexao as "Número Conexão", 
ee.mes as "Mes de Envio do E-Mail",
case when ee.id is not null then cli.email else 'SEM ENVIO DE E-MAIL PARA O MÊS' end as "E-mail Enviados",
tt.id as "ID Tabela Preço"
from contratos ct
join cont_prog cp on cp.cidade=ct.cidade and cp.contrato=ct.contrato and cp.codempresa=ct.codempresa 
join tabeladeprecos tt on tt.codcidade=cp.cidade and tt.codigo=cp.codigodatabeladeprecos
join programacao p on p.codigodaprogramacao=cp.protabelaprecos and p.codcidade=cp.cidade 
join clientes cli on cli.cidade=ct.cidade and cli.codigocliente=ct.codigodocliente 
join cidade ci on ci.codigodacidade=ct.cidade 
join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
left join tiposdevenda tv on tv.codigo=ct.tipodevenda
left join email ee on ee.id=ct.id
where ct.faturaimpressa = 1 --limit 10000


--*******************************************************************************************
-- Impressões - Envio Fisico de Boletos
--*******************************************************************************************
with 
  via2 as (
     select c.nossonumero, count(*) as qtde
     from controlereimpressoes c
     where c.tiporeimpressao in (1) and c.d_datacadastro between '2022-08-01' and current_date
     and lower(c.usuario) <> 'reguacobranca'
     group by c.nossonumero
   ),
   via2ura as (
     select c.nossonumero, count(*) as qtde
     from controlereimpressoes c
     where c.tiporeimpressao in (9) and c.d_datacadastro between '2022-08-01' and current_date
     and lower(c.usuario) <> 'reguacobranca'
     group by c.nossonumero
   ),
   grafica as (
     select distinct c.nossonumero
     from controlereimpressoes c
     where c.tiporeimpressao in (3) and c.d_datacadastro between '2022-08-01' and '2022-08-31'
)  
select distinct cli.cpf_cnpj as "CPF/CNPJ",
case when ct.formapagamento = 1 and ct.faturaimpressa = 1 then 'Boleto Bancário Digital'
     when ct.formapagamento = 1 and ct.faturaimpressa = 2 then 'Boleto Bancário Impresso'
     when ct.formapagamento = 2 then 'Deposito Identificado'
     when ct.formapagamento = 3 then 'Débito Automático'
     when ct.formapagamento = 4 then 'Cartão de Crédito'
End as "Tipo Documento",
cli.nome as "Nome Cliente",
ct.bairrocobranca as "Bairro Cobrança",
cli.codigocliente as "Código Cliente",
ci.nomedacidade as "Cidade",
ct.bairroconexao as "Bairro Conexão",
case when  ct.formapagamento = 1 then 'Boleto'
     when  ct.formapagamento = 2 then 'Deposito'
     when  ct.formapagamento = 3 then 'Debito'
     when  ct.formapagamento = 4 then 'Cartao de Credito'
end as "Local Cobrança",
tv.descricao as "Canal de Contratação",
e.tipodologradouro ||' '|| e.nomelogradouro ||  ', Nº'|| ct.numeroconexao AS "Local de Instalacao",
ct.id as "Código Conexão",
p.id as "Código Plano",
p.nomedaprogramacao as "Nome Plano",
ct.numeroconexao as "Número Conexão", 
e1.qtde as "Solicitações de 2ºvia",
e9.qtde as "Solicitações de URA/WHASAPP",
tt.id as "ID Tabela Preço"
from docreceber dr
join grafica c on dr.nossonumero=c.nossonumero
join movimfinanceiro m on m.numfatura=dr.fatura
join contratos ct on ct.cidade=m.cidade and ct.codempresa=m.codempresa and ct.contrato=m.contrato
join cont_prog cp on cp.cidade=ct.cidade and cp.contrato=ct.contrato and cp.codempresa=ct.codempresa 
join tabeladeprecos tt on tt.codcidade=cp.cidade and tt.codigo=cp.codigodatabeladeprecos
join programacao p on p.codigodaprogramacao=cp.protabelaprecos and p.codcidade=cp.cidade 
join clientes cli on cli.cidade=ct.cidade and cli.codigocliente=ct.codigodocliente 
join cidade ci on ci.codigodacidade=ct.cidade 
join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
left join tiposdevenda tv on tv.codigo=ct.tipodevenda
left join via2 e1 on e1.nossonumero=c.nossonumero 
left join via2ura e9 on e9.nossonumero=c.nossonumero
where (e1.nossonumero is not null or e9.nossonumero is not NULL)




--*******************************************************************************************
-- Campanhas promocionais / Ofertas
--*******************************************************************************************
select distinct cli.cpf_cnpj as "CPF/CNPJ",
cli.nome as "Nome Cliente",
cli.codigocliente as "Código Cliente",
ct.bairroconexao as "Bairro Conexão",
e.tipodologradouro ||' '|| e.nomelogradouro ||  ', Nº'|| ct.numeroconexao AS "Local de Instalacao",
ct.numeroconexao as "Número Conexão", 
ci.nomedacidade as "Cidade",
p.nomedaprogramacao as "Pacote",
p.id as "Código Pacote",
cp.valorpacote as "Valor do Pacote",
p.nomedaprogramacao as "Produto",
p.id as "Código Produto",
p.nomedaprogramacao as "Descrição do Produto",
cp.valorpacote as "Preço do Produto",
dp.descricaotipoprogramacao as "Tipo do Plano",
'' as "Descrição do Plano",
'' as "Plano Contratado",
cp.valorpacote as "Preço do PLano",
cp.d_dataativacao as "Data Criação Plano",
array_to_string(ARRAY(
  select a.descricao
  from contratos ct1
  join aditivoscontratos ad on ad.codcidade=ct1.cidade and ad.codempresa=ct1.codempresa and ad.numcontrato=ct1.contrato
  join aditivos a on a.codaditivo=ad.codaditivo
  where ct1.id=ct.id and (('2022-05-31'::date between ad.d_datainicio and ad.d_datafim) or ('2022-05-01'::date between ad.d_datainicio and ad.d_datafim) or '2022-05-01'::date < ad.d_datainicio and '2022-05-31'::date > ad.d_datafim)
  and a.valordesconto > 0
), ' - ', '') as "Nome Tabela/Oferta",
array_to_string(ARRAY(
  select distinct a.codaditivo
  from contratos ct1
  join aditivoscontratos ad on ad.codcidade=ct1.cidade and ad.codempresa=ct1.codempresa and ad.numcontrato=ct1.contrato
  join aditivos a on a.codaditivo=ad.codaditivo
  where ct1.id=ct.id and (('2022-05-31'::date between ad.d_datainicio and ad.d_datafim) or ('2022-05-01'::date between ad.d_datainicio and ad.d_datafim) or '2022-05-01'::date < ad.d_datainicio and '2022-05-31'::date > ad.d_datafim)
  and a.valordesconto > 0
), ' - ', '') as "Código Tabela/Oferta",
array_to_string(ARRAY(
  select distinct ad.d_datainicio
  from contratos ct1
  join aditivoscontratos ad on ad.codcidade=ct1.cidade and ad.codempresa=ct1.codempresa and ad.numcontrato=ct1.contrato
  join aditivos a on a.codaditivo=ad.codaditivo
  where ct1.id=ct.id and (('2022-05-31'::date between ad.d_datainicio and ad.d_datafim) or ('2022-05-01'::date between ad.d_datainicio and ad.d_datafim) or '2022-05-01'::date < ad.d_datainicio and '2022-05-31'::date > ad.d_datafim)
  and a.valordesconto > 0
), ' - ', '') as "Data Criação",
array_to_string(ARRAY(
  select distinct ad.d_datacadastro
  from contratos ct1
  join aditivoscontratos ad on ad.codcidade=ct1.cidade and ad.codempresa=ct1.codempresa and ad.numcontrato=ct1.contrato
  join aditivos a on a.codaditivo=ad.codaditivo
  where ct1.id=ct.id and (('2022-05-31'::date between ad.d_datainicio and ad.d_datafim) or ('2022-05-01'::date between ad.d_datainicio and ad.d_datafim) or '2022-05-01'::date < ad.d_datainicio and '2022-05-31'::date > ad.d_datafim)
  and a.valordesconto > 0
), ' - ', '') as "Liberada em",
array_to_string(ARRAY(
  select distinct ad.d_datafim
  from contratos ct1
  join aditivoscontratos ad on ad.codcidade=ct1.cidade and ad.codempresa=ct1.codempresa and ad.numcontrato=ct1.contrato
  join aditivos a on a.codaditivo=ad.codaditivo
  where ct1.id=ct.id and (('2022-05-31'::date between ad.d_datainicio and ad.d_datafim) or ('2022-05-01'::date between ad.d_datainicio and ad.d_datafim) or '2022-05-01'::date < ad.d_datainicio and '2022-05-31'::date > ad.d_datafim)
  and a.valordesconto > 0
), ' - ', '') as "Data Termino",
array_to_string(ARRAY(
  select distinct a.descricao
  from contratos ct1
  join aditivoscontratos ad on ad.codcidade=ct1.cidade and ad.codempresa=ct1.codempresa and ad.numcontrato=ct1.contrato
  join aditivos a on a.codaditivo=ad.codaditivo
  where ct1.id=ct.id and (('2022-05-31'::date between ad.d_datainicio and ad.d_datafim) or ('2022-05-01'::date between ad.d_datainicio and ad.d_datafim) or '2022-05-01'::date < ad.d_datainicio and '2022-05-31'::date > ad.d_datafim)
  and a.valordesconto > 0
), ' - ', '') as "Descrição Campanha",
regrasoperacao.func_calculavaloraditivos_v2_contrato(ct.id,'2022-05-01'::date,'2022-05-31'::date,cp.valorpacote) as "Valor Campanha Promocional",
array_to_string(ARRAY(
  select distinct a.descricao
  from contratos ct1
  join aditivoscontratos ad on ad.codcidade=ct1.cidade and ad.codempresa=ct1.codempresa and ad.numcontrato=ct1.contrato
  join aditivos a on a.codaditivo=ad.codaditivo
  where ct1.id=ct.id and (('2022-05-31'::date between ad.d_datainicio and ad.d_datafim) or ('2022-05-01'::date between ad.d_datainicio and ad.d_datafim) or '2022-05-01'::date < ad.d_datainicio and '2022-05-31'::date > ad.d_datafim)
  and a.valordesconto > 0
), ' - ', '') as "Campanha Adquirida",
'' as "Margem Produto",
ct.id as "Código Conexão", 
tt.id as "ID Tabeça Preço"
from contratos ct
join cont_prog cp on cp.cidade=ct.cidade and cp.contrato=ct.contrato and cp.codempresa=ct.codempresa 
join tabeladeprecos tt on tt.codcidade=cp.cidade and tt.codigo=cp.codigodatabeladeprecos
join programacao p on p.codigodaprogramacao=cp.protabelaprecos and p.codcidade=cp.cidade
join clientes cli on cli.cidade=ct.cidade and cli.codigocliente=ct.codigodocliente 
join cidade ci on ci.codigodacidade=ct.cidade 
join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
join vis_descritivo_tipo_programacao dp on dp.tipoprogramacao=p.tipoprogramacao
join aditivoscontratos ac on ac.codcidade=ct.cidade and ac.codempresa=ct.codempresa and ac.numcontrato=ct.contrato
join aditivos a on a.codaditivo=ac.codaditivo
left join public.pacotesaditivos pa on pa.codaditivo = a.codaditivo and pa.codcidade = ac.codcidade and pa.codpacote = cp.protabelaprecos
left join public.tipospacotesaditivos tpa on tpa.codaditivo = a.codaditivo and tpa.tipoponto = p.tipoponto and tpa.tipopacote = p.tipoprogramacao
where a.valordesconto > 0 and (pa.id is not null or tpa.id is not null) and
(('2022-05-31'::date between ac.d_datainicio and ac.d_datafim) or ('2022-05-01'::date between ac.d_datainicio and ac.d_datafim) or '2022-05-01'::date < ac.d_datainicio and '2022-05-31'::date > ac.d_datafim)




--*******************************************************************************************
-- RETENÇÃO
--*******************************************************************************************
with
  alt as (
  -- Alteração de Programação
  with 
  ativ as (
     select w.cidade, w.codempresa, w.contrato, w.numos, sum(w.valorpacote_ativacao) as valorpacote_ativacao, sum(w.valor_desconto_ativacao) as valor_desconto_ativacao, w.pacote_ativacao, w.codigo_pacote_ativacao
     from (
       select t.cidade, t.codempresa, t.contrato, t.numos, t.valorpacote_ativacao, t.valor_desconto_ativacao,
       array_to_string(ARRAY(
          select distinct p.nomedaprogramacao
          from variacaodepacotes v 
          join programacao p on p.codcidade=v.cidade and p.codigodaprogramacao=v.pacote
          where v.cidade=t.cidade and v.codempresa=t.codempresa and v.numos=t.numos and v.operacao = 1
       ), ' - ', '') as pacote_ativacao ,
       array_to_string(ARRAY(
          select distinct p.id
          from variacaodepacotes v 
          join programacao p on p.codcidade=v.cidade and p.codigodaprogramacao=v.pacote
          where v.cidade=t.cidade and v.codempresa=t.codempresa and v.numos=t.numos and v.operacao = 1
       ), ' - ', '') as codigo_pacote_ativacao 
       from (
         select v.cidade, v.codempresa, v.contrato, v.numos, 
         case when v.operacao = 1 then p.nomedaprogramacao else '' end as pacote_ativacao,
         case when v.operacao = 1 then v.valorpacote else 0 end as valorpacote_ativacao,
         case when v.operacao = 1 then public.func_calculavaloraditivos_v2(
                   v.cidade, v.codempresa, v.contrato,
                   p.tipoponto::integer, p.tipoprogramacao::integer, v.valorpacote - (v.valorpacote * tc.desconto / 100),
                   '2022-05-01'::date,'2022-05-31'::date, v.pacote::integer) else 0 end as valor_desconto_ativacao
         from variacaodepacotes v 
         join programacao p on p.codcidade=v.cidade and p.codigodaprogramacao=v.pacote
         join contratos ct on ct.cidade=v.cidade and ct.codempresa=v.codempresa and ct.contrato=v.contrato
         join tiposcontrato tc on tc.codigo=ct.tipodocontrato
         where v.operacao = 1 and v.d_data between '2022-05-01'::date and '2022-05-31'::date
        ) as t
     ) as w
     group by w.cidade, w.codempresa, w.contrato, w.numos, w.pacote_ativacao, w.codigo_pacote_ativacao
  ),
  dest as (
    select w.cidade, w.codempresa, w.contrato, w.numos, sum(w.valorpacote_desativacao) as valorpacote_desativacao, sum(w.valor_desconto_desativacao) as valor_desconto_desativacao, w.pacote_desativacao, w.codigo_pacote_desativacao
    from (
     select t.cidade, t.codempresa, t.contrato, t.numos, t.valorpacote_desativacao, t.valor_desconto_desativacao,
      array_to_string(ARRAY(
         select distinct p.nomedaprogramacao
         from variacaodepacotes v 
         join programacao p on p.codcidade=v.cidade and p.codigodaprogramacao=v.pacote
         where v.cidade=t.cidade and v.codempresa=t.codempresa and v.numos=t.numos and v.operacao = 2
      ), ' - ', '') as pacote_desativacao,
      array_to_string(ARRAY(
         select distinct p.id
         from variacaodepacotes v 
         join programacao p on p.codcidade=v.cidade and p.codigodaprogramacao=v.pacote
         where v.cidade=t.cidade and v.codempresa=t.codempresa and v.numos=t.numos and v.operacao = 2
      ), ' - ', '') as codigo_pacote_desativacao 
      from (
        select v.cidade, v.codempresa, v.contrato, v.numos, 
        case when v.operacao = 2 then p.nomedaprogramacao else '' end as pacote_desativacao,
        case when v.operacao = 2 then v.valorpacote else 0 end as valorpacote_desativacao,
        case when v.operacao = 2 then public.func_calculavaloraditivos_v2(
                  v.cidade, v.codempresa, v.contrato,
                  p.tipoponto::integer, p.tipoprogramacao::integer, v.valorpacote - (v.valorpacote * tc.desconto / 100),
                  '2022-05-01'::date,'2022-05-31'::date, v.pacote::integer) else 0 end as valor_desconto_desativacao
        from variacaodepacotes v 
        join programacao p on p.codcidade=v.cidade and p.codigodaprogramacao=v.pacote
        join contratos ct on ct.cidade=v.cidade and ct.codempresa=v.codempresa and ct.contrato=v.contrato
        join tiposcontrato tc on tc.codigo=ct.tipodocontrato
        where v.operacao = 2 and v.d_data between '2022-05-01'::date and '2022-05-31'::date
     ) as t
   ) as w
   group by w.cidade, w.codempresa, w.contrato, w.numos, w.pacote_desativacao, w.codigo_pacote_desativacao
  ),
  ordserv as (
    select distinct os.cidade, os.codempresa, os.numos
    from public.ordemservico os
    join public.lanceservicos l on l.codigodoserv_lanc = os.codservsolicitado
    where os.d_dataexecucao between '2022-05-01'::date and '2022-05-31'::date and l.baixapontosmarcados = 4
  )
  select his.controle, a.pacote_ativacao, a.valorpacote_ativacao, d.pacote_desativacao, d.valorpacote_desativacao, a.codigo_pacote_ativacao, d.codigo_pacote_desativacao
  from ordserv oo
  join ordemservico os on os.cidade=oo.cidade and os.codempresa=oo.codempresa and os.numos=oo.numos
  join contratos ct on ct.cidade=os.cidade and ct.codempresa=os.codempresa and ct.contrato=os.codigocontrato
  join historicogeral his on his.codigocidade=os.cidade and his.ordemservico=os.numos
  join grupohistorico g on g.codigo=his.grupoassunto
  join lanceservicos l on l.codigodoserv_lanc=os.codservsolicitado
  left join ativ a on a.cidade=oo.cidade and a.codempresa=oo.codempresa and a.numos=oo.numos
  left join dest d on d.cidade=a.cidade and d.codempresa=a.codempresa and d.numos=a.numos
  where l.baixapontosmarcados = 4 and to_ascii(g.descricao) ilike '%retencao%' and his.d_data between '2022-05-01' and '2022-05-31'
)
select distinct cli.cpf_cnpj as "CPF/CNPJ",
cli.nome as "Nome Cliente",
cli.codigocliente as "Código Cliente",
ct.cepcobranca as "CEP Cobrança",
ct.cepconexao as "CEP Instalação",
ci.nomedacidade as "Cidade",
tc.nome as "Cidade Cobrança",
ttc.descricao as "Classificação Comercial",
ci.codigodacidade as "Código Cidade",
ct.id as "Código da Conexão",
his.controle as "Código da Movimentação",
al.pacote_ativacao as "Novo Plano",
al.valorpacote_ativacao as "Valor Novo Plano",
al.pacote_desativacao as "Plano Anterior",
al.valorpacote_desativacao as "Valor Novo Anterior",
al.codigo_pacote_desativacao as "Código do Produto",
his.d_data as "Data Execução da Movimentação",
his.d_data as "Data Solicitação de Movimentação",
ct.d_datadavenda as "Data Inicio do Relacionamento",
ct.d_datadainstalacao as "Data Instalação",
cli.d_datanascimento as "Data Nascimento",
'' as "Data Desconto",
v.descricaosituacao as "Status Cliente",
his.d_data as "Data Status Cliente",
'' as "Movimentação Cadastro",
'' as "Movimentações do Produto",
cli.nome as "Nome do Cliente",
ct.numeroconexao as "Número Conexão", 
'' as "Percentual de Desconto",
0 as "Qtde Planos Contratados",
(
  select count(*) 
  from contratos ct1
  join cont_prog cp1 on cp1.cidade=ct.cidade and cp1.contrato=ct.contrato and cp1.codempresa=ct.codempresa 
  join programacao pr1 on pr1.codcidade=cp1.cidade and pr1.codigodaprogramacao=cp1.protabelaprecos
  where ct1.id=ct.id and pr1.tipoponto in (2,3) and pr1.tipoprogramacao in (0,2,3,4,5,6,7,8,9)
) as "Qtde Produtos Contratados",
case when cli.tipoassinante = 1 then 'Comercial/Industrial'
     when cli.tipoassinante = 2 then 'Poder Publico'
     when cli.tipoassinante = 3 then 'Residencial/Pessoa Fisica'
     when cli.tipoassinante = 4 then 'Publico'
     when cli.tipoassinante = 5 then 'Semi-Publico'
     when cli.tipoassinante = 6 then 'Outros'
end as "Segmento do cliente", 
current_date - ct.d_datadainstalacao as "Tempo de Relacionamento (dias)",
'' as "Tipo Cliente",
case when ct.formapagamento = 1 and ct.faturaimpressa = 1 then 'Boleto Bancário Digital'
     when ct.formapagamento = 1 and ct.faturaimpressa = 2 then 'Boleto Bancário Impresso'
     when ct.formapagamento = 2 then 'Deposito Identificado'
     when ct.formapagamento = 3 then 'Débito Automático'
     when ct.formapagamento = 4 then 'Cartão de Crédito'
End as "Tipo Documento",
al.valorpacote_desativacao - al.valorpacote_ativacao as "Valor Desconto",
cp.valorpacote as "Valor do Plano",
aa.codaditivo as "Código Campanha Adquirida",
aa.descricao as "Nome Campanha Adquirida",
case when aa.codaditivo is not null then ad.d_datainicio else null end as "Inicio Campanha",
case when aa.codaditivo is not null then ad.d_datafim else null end as "Fim Campanha",
aa.valordesconto as "Desconto Campanha", tt.id as "ID Tabela Preço",
p.id as "Código Produto Atual",
p.nomedaprogramacao as "Nome Produto Atual",
cp.valorpacote as "Valor Produto Atual"
from contratos ct
join cont_prog cp on cp.cidade=ct.cidade and cp.contrato=ct.contrato and cp.codempresa=ct.codempresa 
join tabeladeprecos tt on tt.codcidade=cp.cidade and tt.codigo=cp.codigodatabeladeprecos
join programacao p on p.codigodaprogramacao=cp.protabelaprecos and p.codcidade=cp.cidade 
join clientes cli on cli.cidade=ct.cidade and cli.codigocliente=ct.codigodocliente 
join cidade ci on ci.codigodacidade=ct.cidade 
join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
join enderecos ec on ec.codigodacidade=ct.cidadecobranca and ec.codigodologradouro=ct.enderecodecobranca
join tablocal tc on tc.codigo=ct.cidadecobranca
join vis_situacaocontrato_descricao v on v.situacao=ct.situacao
join vis_descritivo_tipo_programacao dp on dp.tipoprogramacao=p.tipoprogramacao
join historicogeral his on his.codigocidade=ct.cidade and his.codempresa=ct.codempresa and his.codcontrato=ct.contrato
join grupohistorico g on g.codigo=his.grupoassunto
join tiposcontrato ttc on ttc.codigo=ct.tipodocontrato
left join alt al on al.controle = his.controle
left join aditivoscontratos ad on ad.codcidade=ct.cidade and ad.codempresa=ct.codempresa and ad.numcontrato=ct.contrato and ad.d_datacadastro = his.d_datacadastro
left join aditivos aa on aa.codaditivo=ad.codaditivo --and aa.valordesconto > 0
where to_ascii(g.descricao) ilike '%retencao%' and his.d_data between '2022-05-01' and '2022-05-31'





--*******************************************************************************************
-- RECLAMAÇÕES
--*******************************************************************************************
select cid.nomedacidade as "Cidade", 
cli.codigocliente as "Código Cliente", 
cli.nome as "Nome Cliente", 
ct.id as "ID Conexão", 
h.d_data as "Data Reclamaçaõ",
h.controle as "ID Reclamação",
g.descricao as "Grupo Histórico",
a.descricao as "Assunto Histórico",
h.descricao as "Descrição da Reclamação"
from historicogeral h
join clientes cli on cli.cidade=h.codigocidade and cli.codigocliente=h.assinante
join contratos ct on ct.cidade=h.codigocidade and ct.codempresa=h.codempresa and ct.contrato=h.codcontrato
join cidade cid on cid.codigodacidade = h.codigocidade
join grupohistorico g on g.codigo=h.grupoassunto
join assuntohistorico a on a.codigogrupo=h.grupoassunto and a.codigoassunto=h.assunto
where h.d_data between '2022-05-01' and '2022-09-30'
and a.descricao ilike '%reclama%'




--*******************************************************************************************
-- SOLICITAÇÕES DE 2º
--*******************************************************************************************
select distinct cid.nomedacidade as "Cidade",
cli.cidade as "Código Cliente",
cli.nome as "Nome Cliente",
ct.id as "ID Conexão",
case when c.tiporeimpressao = 1 then 'Impresso'
     when c.tiporeimpressao = 2 then 'Enviado por E-mail'
     when c.tiporeimpressao = 9 then 'Email Boleto PDF'
     when c.tiporeimpressao = 10 then 'Copiada linha digitável'
     when c.tiporeimpressao = 11 then 'Enviada linha digitável por email '
     when c.tiporeimpressao = 12 then 'Copiada URL do boleto PDF'
end as "Tipo Envio",
case when upper(btrim(c.usuario)) = 'USERAPI' then 'URA/WHASTAPP' else 'CALL CENTER' end as "Forma Envio",
c.d_datacadastro as "Data Envio"
from controlereimpressoes c
join docreceber dr on dr.nossonumero=c.nossonumero
join movimfinanceiro m on m.numfatura=dr.fatura
join contratos ct on ct.cidade=m.cidade and ct.codempresa=m.codempresa and ct.contrato=m.contrato
join clientes cli on cli.cidade=ct.cidade and cli.codigocliente=ct.codigodocliente
join cidade cid on cid.codigodacidade = ct.cidade
where c.d_datacadastro between '2022-05-01' and '2022-05-05'
and c.tiporeimpressao in (1,2,9,10,11,12) and lower(btrim(c.usuario)) not in ('reguacobranca')