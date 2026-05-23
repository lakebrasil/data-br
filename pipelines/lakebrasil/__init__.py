"""lakebrasil — Brazilian public data extraction engine.

35 pipelines extracting and aggregating data from 30+ Brazilian
government sources (IBGE, MTE, MEC, INEP, INPE, ANS, BPC, Bolsa Família,
RAIS, CNPJ Receita, TSE, Câmara, BACEN, ANP, ANEEL, SNIS/SINISA, FNDE,
STN, etc.) into an S3 + Iceberg lakehouse keyed by município.

Programmatic use:
    from lakebrasil.pipelines import bpc, rais, sinisa
    bpc.main()  # CLI entry point of each pipeline

CLI use:
    lakebrasil list                  # list all pipelines
    lakebrasil run bpc --no-fetch    # run a pipeline
    lakebrasil dq                    # run data quality checks

Source code: https://github.com/lakebrasil/data-br
License: MIT
"""
__version__ = "0.1.0"
