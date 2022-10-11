--*******************************************************************************************
--Faturamento - Complemento de NF Geradas Posteriores
--*******************************************************************************************
with 
  nf as (
    select distinct d.id
    from nfviaunica nf  
    join docreceber d on d.id=nf.idboleto
    where nf.d_dataemissao between '2022-06-01' and current_date
    and d.d_datafaturamento < '2022-05-31'
  )
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
join nf n on n.id=dr.id
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