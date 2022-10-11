-- Alteração de Programação
select * from (
  with
  ativ as (
     select w.cidade, w.codempresa, w.contrato, w.numos, sum(w.valorpacote_ativacao) as valorpacote_ativacao, sum(w.valor_desconto_ativacao) as valor_desconto_ativacao, w.pacote_ativacao
     from (
       select t.cidade, t.codempresa, t.contrato, t.numos, t.valorpacote_ativacao, t.valor_desconto_ativacao,
       array_to_string(ARRAY(
          select distinct p.nomedaprogramacao
          from variacaodepacotes v
          join programacao p on p.codcidade=v.cidade and p.codigodaprogramacao=v.pacote
          where v.cidade=t.cidade and v.codempresa=t.codempresa and v.numos=t.numos
       ), ' - ', '') as pacote_ativacao
       from (
         select v.cidade, v.codempresa, v.contrato, v.numos,
         case when v.operacao = 1 then p.nomedaprogramacao else '' end as pacote_ativacao,
         case when v.operacao = 1 then v.valorpacote else 0 end as valorpacote_ativacao,
         case when v.operacao = 1 then public.func_calculavaloraditivos_v2(
                   v.cidade, v.codempresa, v.contrato,
                   p.tipoponto::integer, p.tipoprogramacao::integer, v.valorpacote - (v.valorpacote * tc.desconto / 100),
                   '2022-06-01'::date,'2022-06-30'::date, v.pacote::integer) else 0 end as valor_desconto_ativacao
         from variacaodepacotes v
         join programacao p on p.codcidade=v.cidade and p.codigodaprogramacao=v.pacote
         join contratos ct on ct.cidade=v.cidade and ct.codempresa=v.codempresa and ct.contrato=v.contrato
         join tiposcontrato tc on tc.codigo=ct.tipodocontrato
         where v.operacao = 1 and v.d_data between '2022-06-01'::date and '2022-06-30'::date
        ) as t
     ) as w
     group by w.cidade, w.codempresa, w.contrato, w.numos, w.pacote_ativacao
  ),
  dest as (
    select w.cidade, w.codempresa, w.contrato, w.numos, sum(w.valorpacote_desativacao) as valorpacote_desativacao, sum(w.valor_desconto_desativacao) as valor_desconto_desativacao, w.pacote_desativacao
    from (
     select t.cidade, t.codempresa, t.contrato, t.numos, t.valorpacote_desativacao, t.valor_desconto_desativacao,
      array_to_string(ARRAY(
         select distinct p.nomedaprogramacao
         from variacaodepacotes v
         join programacao p on p.codcidade=v.cidade and p.codigodaprogramacao=v.pacote
         where v.cidade=t.cidade and v.codempresa=t.codempresa and v.numos=t.numos
      ), ' - ', '') as pacote_desativacao
      from (
        select v.cidade, v.codempresa, v.contrato, v.numos,
        case when v.operacao = 2 then p.nomedaprogramacao else '' end as pacote_desativacao,
        case when v.operacao = 2 then v.valorpacote else 0 end as valorpacote_desativacao,
        case when v.operacao = 2 then public.func_calculavaloraditivos_v2(
                  v.cidade, v.codempresa, v.contrato,
                  p.tipoponto::integer, p.tipoprogramacao::integer, v.valorpacote - (v.valorpacote * tc.desconto / 100),
                  '2022-06-01'::date,'2022-06-30'::date, v.pacote::integer) else 0 end as valor_desconto_desativacao
        from variacaodepacotes v
        join programacao p on p.codcidade=v.cidade and p.codigodaprogramacao=v.pacote
        join contratos ct on ct.cidade=v.cidade and ct.codempresa=v.codempresa and ct.contrato=v.contrato
        join tiposcontrato tc on tc.codigo=ct.tipodocontrato
        where v.operacao = 2 and v.d_data between '2022-06-01'::date and '2022-06-30'::date
     ) as t
   ) as w
   group by w.cidade, w.codempresa, w.contrato, w.numos, w.pacote_desativacao
  ),
  ordserv as (
    select distinct os.cidade, os.codempresa, os.numos
    from public.ordemservico os
    join public.lanceservicos l on l.codigodoserv_lanc = os.codservsolicitado
    where os.d_dataexecucao between '2022-06-01'::date and '2022-06-30'::date and l.baixapontosmarcados = 4
  )
  select
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
  cid.nomedacidade as "Cidade",
  ct.bairroconexao as "Bairro Conexão",
  case when  ct.formapagamento = 1 then 'Boleto'
       when  ct.formapagamento = 2 then 'Deposito'
       when  ct.formapagamento = 3 then 'Debito'
       when  ct.formapagamento = 4 then 'Cartao de Credito'
  end as "Local de Cobrança",
  tv.descricao as "Canal de contratação",
  e.tipodologradouro ||' '|| e.nomelogradouro ||  ', Nº'|| ct.numeroconexao AS "Local de Instalacao", ct.numeroconexao as "Nº Conexão",
  ec.tipodologradouro ||' '|| ec.nomelogradouro ||  ', Nº'|| ct.numerocobranca AS "Local de Cobrança", ct.numerocobranca as "Nº Cobrança",
  p.nomedaprogramacao as "Produto Atual",
  cp.valorpacote as "Preco do Produto Atual",
  ct.id as "Código Conexão",
  os.id as "Código Movimentação",
  p.id as "Código do Plano",
  ct.d_datadainstalacao as "Data Instalação",
  ct.d_datadavenda as "Data Venda",
  0 as "Descontos e Ofertas",
  0 as "Margem do Produto",
  ct.numeroconexao as "Nº Conexão",
  a.pacote_ativacao as "Plano Contratado",
  a.valorpacote_ativacao as "Preço do Plano Contratado",
  d.pacote_desativacao as "Plano Desconectado",
  d.valorpacote_desativacao as "Valor Plano Desconectado",
  0 as "Termos ou Condições Especiais",
  case when  ct.formapagamento = 1 then 'Boleto'
       when  ct.formapagamento = 2 then 'Deposito'
       when  ct.formapagamento = 3 then 'Debito'
       when  ct.formapagamento = 4 then 'Cartao de Credito'
  end as "Pagamento Selecionado",
  0 as "Tipo do Plano", 'Movimentação - Alteração de Programação',
  os.d_dataexecucao as "Data Movimentação",
  aa.codaditivo as "Código Campanha Adquirida",
  aa.descricao as "Nome Campanha Adquirida",
  case when aa.codaditivo is not null then ad.d_datainicio else null end as "Inicio Campanha",
  case when aa.codaditivo is not null then ad.d_datafim else null end as "Fim Campanha",
  aa.valordesconto as "Desconto Campanha",
  t.id as "ID Tabela Preço"
  from ordserv oo
  join ordemservico os on os.cidade=oo.cidade and os.codempresa=oo.codempresa and os.numos=oo.numos
  join contratos ct on ct.cidade=os.cidade and ct.codempresa=os.codempresa and ct.contrato=os.codigocontrato and ct.situacao <> 5
  join cidade cid on cid.codigodacidade=os.cidade
  join clientes cli on cli.cidade=os.cidade and cli.codigocliente=os.codigoassinante
  join cont_prog cp on cp.cidade=ct.cidade and cp.contrato=ct.contrato and cp.codempresa=ct.codempresa
  join tabeladeprecos t on t.codcidade=cp.cidade and t.codigo=cp.codigodatabeladeprecos
  join programacao p on p.codigodaprogramacao=cp.protabelaprecos and p.codcidade=cp.cidade
  join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
  join enderecos ec on ec.codigodacidade=ct.cidadecobranca and ec.codigodologradouro=ct.enderecodecobranca
  join public.lanceservicos l on l.codigodoserv_lanc = os.codservsolicitado
  left join tiposdevenda tv on tv.codigo=ct.tipodevenda
  left join ativ a on a.cidade=oo.cidade and a.codempresa=oo.codempresa and a.numos=oo.numos
  left join dest d on d.cidade=a.cidade and d.codempresa=a.codempresa and d.numos=a.numos
  left join aditivoscontratos ad on ad.codcidade=ct.cidade and ad.codempresa=ct.codempresa and ad.numcontrato=ct.contrato and ad.d_datacadastro=os.d_dataexecucao
  left join aditivos aa on aa.codaditivo=ad.codaditivo and aa.valordesconto > 0
  where l.baixapontosmarcados = 4 and os.d_dataexecucao between '2022-06-01' and '2022-06-30'
) as x
--and  cli.codigocliente between 312591 and 613591 and cli.cidade = 121491
union
-- Inclusão de Plano
select * from (
  with
  ativ as (
     select w.cidade, w.codempresa, w.contrato, w.numos, sum(w.valorpacote_ativacao) as valorpacote_ativacao, sum(w.valor_desconto_ativacao) as valor_desconto_ativacao, w.pacote_ativacao
     from (
       select t.cidade, t.codempresa, t.contrato, t.numos, t.valorpacote_ativacao, t.valor_desconto_ativacao,
       array_to_string(ARRAY(
          select distinct p.nomedaprogramacao
          from variacaodepacotes v
          join programacao p on p.codcidade=v.cidade and p.codigodaprogramacao=v.pacote
          where v.cidade=t.cidade and v.codempresa=t.codempresa and v.numos=t.numos
       ), ' - ', '') as pacote_ativacao
       from (
         select v.cidade, v.codempresa, v.contrato, v.numos,
         case when v.operacao = 1 then p.nomedaprogramacao else '' end as pacote_ativacao,
         case when v.operacao = 1 then v.valorpacote else 0 end as valorpacote_ativacao,
         case when v.operacao = 1 then public.func_calculavaloraditivos_v2(
                   v.cidade, v.codempresa, v.contrato,
                   p.tipoponto::integer, p.tipoprogramacao::integer, v.valorpacote - (v.valorpacote * tc.desconto / 100),
                   '2022-06-01'::date,'2022-06-30'::date, v.pacote::integer) else 0 end as valor_desconto_ativacao
         from variacaodepacotes v
         join programacao p on p.codcidade=v.cidade and p.codigodaprogramacao=v.pacote
         join contratos ct on ct.cidade=v.cidade and ct.codempresa=v.codempresa and ct.contrato=v.contrato
         join tiposcontrato tc on tc.codigo=ct.tipodocontrato
         where v.operacao = 1 and v.d_data between '2022-06-01'::date and '2022-06-30'::date
        ) as t
     ) as w
     group by w.cidade, w.codempresa, w.contrato, w.numos, w.pacote_ativacao
  ),
  ordserv as (
    select distinct os.cidade, os.codempresa, os.numos
    from public.ordemservico os
    join public.lanceservicos l on l.codigodoserv_lanc = os.codservsolicitado
    where os.d_dataexecucao between '2022-06-01'::date and '2022-06-30'::date and l.baixapontosmarcados = 2
  )
  select
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
  cid.nomedacidade as "Cidade",
  ct.bairroconexao as "Bairro Conexão",
  case when  ct.formapagamento = 1 then 'Boleto'
       when  ct.formapagamento = 2 then 'Deposito'
       when  ct.formapagamento = 3 then 'Debito'
       when  ct.formapagamento = 4 then 'Cartao de Credito'
  end as "Local de Cobrança",
  tv.descricao as "Canal de contratação",
  e.tipodologradouro ||' '|| e.nomelogradouro ||  ', Nº'|| ct.numeroconexao AS "Local de Instalacao", ct.numeroconexao as "Nº Conexão",
  ec.tipodologradouro ||' '|| ec.nomelogradouro ||  ', Nº'|| ct.numerocobranca AS "Local de Cobrança", ct.numerocobranca as "Nº Cobrança",
  p.nomedaprogramacao as "Produto Atual",
  cp.valorpacote as "Preco do Produto Atual",
  ct.id as "Código Conexão",
  os.id as "Código Movimentação",
  p.id as "Código do Plano",
  ct.d_datadainstalacao as "Data Instalação",
  ct.d_datadavenda as "Data Venda",
  0 as "Descontos e Ofertas",
  0 as "Margem do Produto",
  ct.numeroconexao as "Nº Conexão",
  a.pacote_ativacao as "Plano Contratado",
  a.valorpacote_ativacao as "Preço do Plano Contratado",
  ' ' as "Plano Desconectado",
  0 as "Valor Plano Desconectado",
  0 as "Termos ou Condições Especiais",
  case when  ct.formapagamento = 1 then 'Boleto'
       when  ct.formapagamento = 2 then 'Deposito'
       when  ct.formapagamento = 3 then 'Debito'
       when  ct.formapagamento = 4 then 'Cartao de Credito'
  end as "Pagamento Selecionado",
  0 as "Tipo do Plano", 'Movimentação - Inclusão de Programação',
  os.d_dataexecucao as "Data Movimentação",
  aa.codaditivo as "Código Campanha Adquirida",
  aa.descricao as "Nome Campanha Adquirida",
  case when aa.codaditivo is not null then ad.d_datainicio else null end as "Inicio Campanha",
  case when aa.codaditivo is not null then ad.d_datafim else null end as "Fim Campanha",
  aa.valordesconto as "Desconto Campanha",
  t.id as "ID Tabela Preço"
  from ordserv oo
  join ordemservico os on os.cidade=oo.cidade and os.codempresa=oo.codempresa and os.numos=oo.numos
  join contratos ct on ct.cidade=os.cidade and ct.codempresa=os.codempresa and ct.contrato=os.codigocontrato and ct.situacao <> 5
  join cidade cid on cid.codigodacidade=os.cidade
  join clientes cli on cli.cidade=os.cidade and cli.codigocliente=os.codigoassinante
  join cont_prog cp on cp.cidade=ct.cidade and cp.contrato=ct.contrato and cp.codempresa=ct.codempresa
  join tabeladeprecos t on t.codcidade=cp.cidade and t.codigo=cp.codigodatabeladeprecos
  join programacao p on p.codigodaprogramacao=cp.protabelaprecos and p.codcidade=cp.cidade
  join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
  join enderecos ec on ec.codigodacidade=ct.cidadecobranca and ec.codigodologradouro=ct.enderecodecobranca
  join public.lanceservicos l on l.codigodoserv_lanc = os.codservsolicitado
  left join tiposdevenda tv on tv.codigo=ct.tipodevenda
  join ativ a on a.cidade=oo.cidade and a.codempresa=oo.codempresa and a.numos=oo.numos
  left join aditivoscontratos ad on ad.codcidade=ct.cidade and ad.codempresa=ct.codempresa and ad.numcontrato=ct.contrato and ad.d_datacadastro=os.d_dataexecucao
  left join aditivos aa on aa.codaditivo=ad.codaditivo and aa.valordesconto > 0
  where l.baixapontosmarcados = 2  and os.codservsolicitado <> 11 and os.d_dataexecucao between '2022-06-01' and '2022-06-30'
) as w
union
-- Desativação de Plano
select * from (
  with
  dest as (
    select w.cidade, w.codempresa, w.contrato, w.numos, sum(w.valorpacote_desativacao) as valorpacote_desativacao, sum(w.valor_desconto_desativacao) as valor_desconto_desativacao, w.pacote_desativacao
    from (
     select t.cidade, t.codempresa, t.contrato, t.numos, t.valorpacote_desativacao, t.valor_desconto_desativacao,
      array_to_string(ARRAY(
         select distinct p.nomedaprogramacao
         from variacaodepacotes v
         join programacao p on p.codcidade=v.cidade and p.codigodaprogramacao=v.pacote
         where v.cidade=t.cidade and v.codempresa=t.codempresa and v.numos=t.numos
      ), ' - ', '') as pacote_desativacao
      from (
        select v.cidade, v.codempresa, v.contrato, v.numos,
        case when v.operacao = 2 then p.nomedaprogramacao else '' end as pacote_desativacao,
        case when v.operacao = 2 then v.valorpacote else 0 end as valorpacote_desativacao,
        case when v.operacao = 2 then public.func_calculavaloraditivos_v2(
                  v.cidade, v.codempresa, v.contrato,
                  p.tipoponto::integer, p.tipoprogramacao::integer, v.valorpacote - (v.valorpacote * tc.desconto / 100),
                  '2022-06-01'::date,'2022-06-30'::date, v.pacote::integer) else 0 end as valor_desconto_desativacao
        from variacaodepacotes v
        join programacao p on p.codcidade=v.cidade and p.codigodaprogramacao=v.pacote
        join contratos ct on ct.cidade=v.cidade and ct.codempresa=v.codempresa and ct.contrato=v.contrato
        join tiposcontrato tc on tc.codigo=ct.tipodocontrato
        where v.operacao = 2 and v.d_data between '2022-06-01'::date and '2022-06-30'::date
     ) as t
   ) as w
   group by w.cidade, w.codempresa, w.contrato, w.numos, w.pacote_desativacao
  ),
  ordserv as (
    select distinct os.cidade, os.codempresa, os.numos
    from public.ordemservico os
    join public.lanceservicos l on l.codigodoserv_lanc = os.codservsolicitado
    where os.d_dataexecucao between '2022-06-01'::date and '2022-06-30'::date and l.baixapontosmarcados = 3
  )
  select
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
  cid.nomedacidade as "Cidade",
  ct.bairroconexao as "Bairro Conexão",
  case when  ct.formapagamento = 1 then 'Boleto'
       when  ct.formapagamento = 2 then 'Deposito'
       when  ct.formapagamento = 3 then 'Debito'
       when  ct.formapagamento = 4 then 'Cartao de Credito'
  end as "Local de Cobrança",
  tv.descricao as "Canal de contratação",
  e.tipodologradouro ||' '|| e.nomelogradouro ||  ', Nº'|| ct.numeroconexao AS "Local de Instalacao", ct.numeroconexao as "Nº Conexão",
  ec.tipodologradouro ||' '|| ec.nomelogradouro ||  ', Nº'|| ct.numerocobranca AS "Local de Cobrança", ct.numerocobranca as "Nº Cobrança",
  p.nomedaprogramacao as "Produto Atual",
  cp.valorpacote as "Preco do Produto Atual",
  ct.id as "Código Conexão",
  os.id as "Código Movimentação",
  p.id as "Código do Plano",
  ct.d_datadainstalacao as "Data Instalação",
  ct.d_datadavenda as "Data Venda",
  0 as "Descontos e Ofertas",
  0 as "Margem do Produto",
  ct.numeroconexao as "Nº Conexão",
  ' ' as "Plano Contratado",
  0 as "Preço do Plano Contratado",
  d.pacote_desativacao as "Plano Desconectado",
  d.valorpacote_desativacao as "Valor Plano Desconectado",
  0 as "Termos ou Condições Especiais",
  case when  ct.formapagamento = 1 then 'Boleto'
       when  ct.formapagamento = 2 then 'Deposito'
       when  ct.formapagamento = 3 then 'Debito'
       when  ct.formapagamento = 4 then 'Cartao de Credito'
  end as "Pagamento Selecionado",
  0 as "Tipo do Plano", 'Movimentação - Desativação de Programação',
  os.d_dataexecucao as "Data Movimentação",
  aa.codaditivo as "Código Campanha Adquirida",
  aa.descricao as "Nome Campanha Adquirida",
  case when aa.codaditivo is not null then ad.d_datainicio else null end as "Inicio Campanha",
  case when aa.codaditivo is not null then ad.d_datafim else null end as "Fim Campanha",
  aa.valordesconto as "Desconto Campanha",
  t.id as "ID Tabela Preço"
  from ordserv oo
  join ordemservico os on os.cidade=oo.cidade and os.codempresa=oo.codempresa and os.numos=oo.numos
  join contratos ct on ct.cidade=os.cidade and ct.codempresa=os.codempresa and ct.contrato=os.codigocontrato and ct.situacao <> 5
  join cidade cid on cid.codigodacidade=os.cidade
  join clientes cli on cli.cidade=os.cidade and cli.codigocliente=os.codigoassinante
  join cont_prog cp on cp.cidade=ct.cidade and cp.contrato=ct.contrato and cp.codempresa=ct.codempresa
  join tabeladeprecos t on t.codcidade=cp.cidade and t.codigo=cp.codigodatabeladeprecos
  join programacao p on p.codigodaprogramacao=cp.protabelaprecos and p.codcidade=cp.cidade
  join enderecos e on e.codigodacidade = ct.cidade and e.codigodologradouro = ct.enderecoconexao
  join enderecos ec on ec.codigodacidade=ct.cidadecobranca and ec.codigodologradouro=ct.enderecodecobranca
  join public.lanceservicos l on l.codigodoserv_lanc = os.codservsolicitado
  left join tiposdevenda tv on tv.codigo=ct.tipodevenda
  join dest d on d.cidade=oo.cidade and d.codempresa=oo.codempresa and d.numos=oo.numos
  left join aditivoscontratos ad on ad.codcidade=ct.cidade and ad.codempresa=ct.codempresa and ad.numcontrato=ct.contrato and ad.d_datacadastro=os.d_dataexecucao
  left join aditivos aa on aa.codaditivo=ad.codaditivo and aa.valordesconto > 0
  where l.baixapontosmarcados = 3 and os.d_dataexecucao between '2022-06-01' and '2022-06-30'
) as z