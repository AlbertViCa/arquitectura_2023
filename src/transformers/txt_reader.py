##!/usr/bin/env python
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------
# Archivo: csv_transformer.py
# Capitulo: Flujo de Datos
# Autor(es): Perla Velasco & Yonathan Mtz. & Jorge Solís
# Version: 1.0.0 Noviembre 2022
# Descripción:
#
#   Este archivo define un procesador de datos que se encarga de transformar
#   y formatear el contenido de un archivo TXT
# -------------------------------------------------------------------------
from src.extractors.txt_extractor import TXTExtractor
from os.path import join
import luigi, os, json


class TXTTransformer(luigi.Task):

    def requires(self):
        return TXTExtractor()

    def run(self):
        transaction_list = []
        for file in self.input():
            with file.open() as txt_file:

                next(txt_file)

                data = txt_file.read()

                transactions = data.split(';')

                for transaction in transactions:
                    if transaction:
                        # Split the transaction into its components
                        fields = transaction.split(',')
                        # Store the data in a dictionary
                        transaction_dict = {
                            'numero_da_fatura': fields[0],
                            'codigo_de_inventario': fields[1],
                            'descricao': fields[2],
                            'montante': fields[3],
                            'data_da_fatura': fields[4],
                            'preco_unitario': fields[5],
                            'id_do_cliente': fields[6],
                            'pais': fields[7]
                        }
                        transaction_list.append(transaction_dict)

        with self.output().open('w') as out:
            out.write(json.dumps(transaction_list, indent=4))

    def output(self):
        project_dir = os.path.dirname(os.path.abspath("loader.py"))
        result_dir = join(project_dir, "result")
        return luigi.LocalTarget(join(result_dir, "txt.json"))
