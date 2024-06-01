#
# Copyright (c) 2024 DataZoo GmbH, all rights reserved.
#


from abc import ABC
from typing import Any, Dict, Generator, List, Mapping, Optional, Union

import duckdb
import json
import time

from functools import lru_cache

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    SyncMode,
    Type
)
from airbyte_cdk.sources import Source


class SourceRfcReadTable(Source):
    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        """
        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return AirbyteConnectionStatus: the connection status object, with status SUCCEEDED in case ping succeeded, FAILED otherwise.
        """
        con = self._create_connection_with_erpl(logger, config)
        try:
            con.sql("PRAGMA sap_rfc_ping")
            return AirbyteConnectionStatus(status=Status.SUCCEEDED, message="ERPL connection test succeeded")
        except Exception as e:
            message = str(e)
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"ERPL connection test failed: {message}")

    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        :param logger:  logger object
        """
        selection = config["table_selection"]
        logger.info("ERPL Source Stream Discovery - selection: %s", selection)

        # Create a connection with ERPL extension loaded
        con = self._create_connection_with_erpl(logger, config)
        res = con.sql(f"SELECT * FROM sap_show_tables(TABLENAME='{selection}') ORDER BY 1")
        
        streams = []
        while row := res.fetchmany():
            stream = self._convert_row_to_stream(row[0], logger, config, con)
            streams.append(stream)
        
        return AirbyteCatalog(streams=streams)

    def _convert_row_to_stream(self, row: Mapping[str, Any], logger: AirbyteLogger, config: json, con: duckdb.DuckDBPyConnection) -> AirbyteStream:
        """
        Convert a row from the result of sap_show_tables into an AirbyteStream object.
        :param row: A row from the result of sap_show_tables
        :param logger: The logger object
        :param config: The user input configuration
        :param con: The connection to ERPL
        :return: An AirbyteStream object
        """
        technical_name = row[0]
        text = row[1]
        table_type = row[2] 

        logger.debug("ERPL Source Stream Discovery - stream is: %s", technical_name)
        json_schema = self._create_json_schema_for_table(technical_name, con)

        return AirbyteStream(name=technical_name, text=text, table_type=table_type, json_schema=json_schema, supported_sync_modes=["full_refresh"])

    def _create_json_schema_for_table(self, table_name: str, con: duckdb.DuckDBPyConnection) -> Mapping[str, Any]:
        """
        Create a JSON schema for a table.
        :param table_name: The name of the table
        :param con: The connection to ERPL
        :return: A JSON schema for the table
        """
        schema = con.sql(f"SELECT * FROM sap_describe_fields('{table_name}') ORDER BY 1").fetchall()
        properties = {}
        for row in schema:
            field_name = row[2]
            field_text = row[3]
            field_type = row[4]
            field_length = int(row[5])
            field_decimals = int(row[6])

            properties[field_name] = {
                "type": self._convert_erpl_field_type_to_json_schema_type(field_type),
                "length": field_length,
                "decimals": field_decimals,
                "description": field_text,
            }

        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": properties,
        }
    
    def _convert_erpl_field_type_to_json_schema_type(self, erpl_field_type: str) -> str:
        """
        Convert an ERPL field type to a JSON schema type.
        :param erpl_field_type: The ERPL field type
        :return: The JSON schema type
        """
        type_map = {
            "ACCP": "number",
            "CHAR": "string",
            "CLNT": "string",
            "CURR": "number",
            "CUKY": "string",
            "DATS": "string",
            "DEC": "number",
            "FLTP": "number",
            "INT1": "integer",
            "INT2": "integer",
            "INT4": "integer",
            "LANG": "string",
            "LCHR": "string",
            "LRAW": "string",
            "NUMC": "string",
            "PREC": "integer",
            "QUAN": "number",
            "RAW": "string",
            "RAWSTRING": "string",
            "RSTR": "string",
            "STRING": "string",
            "STRG": "string",
            "SSTR": "string",
            "TIMS": "string",
            "UNIT": "string"
        }

        if erpl_field_type in type_map:
            return type_map[erpl_field_type]
        else:
            raise ValueError(f"Unsupported ERPL field type: {erpl_field_type}")

    def read(self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, any]) -> Generator[AirbyteMessage, None, None]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        :param catalog: The configured catalog.
        :param state: The user provided state.
        :param logger:  logger object
        """
        con = self._create_connection_with_erpl(logger, config)

        logger.debug("Starting ERPL Source read for all streams ...")
        for configured_stream in catalog.streams:
            stream = configured_stream.stream
            for message in self._read_stream(logger, config, stream, con, state):
                yield message
        

    def _read_stream(self, logger: AirbyteLogger, config: json, stream: AirbyteStream, con: duckdb.DuckDBPyConnection, state: Dict[str, any]) -> Generator[AirbyteMessage, None, None]:
        """
        Read a stream from ERPL.
        :param logger: The logger object
        :param config: The user input configuration
        :param stream: The stream to read
        :param con: The connection to ERPL
        :param state: The user provided state
        :return: A generator of AirbyteMessages
        """
        logger.debug("Starting ERPL Source read for stream: %s", stream.name)
        
        res = con.sql(f"SELECT * FROM sap_read_table('{stream.name}')")
        while row := res.fetchmany():
            msg = self._convert_row_to_message(res.columns, row[0], stream)
            yield msg
            
    def _convert_row_to_message(self, columns: List[str], row: List[Any], stream: AirbyteStream) -> AirbyteMessage:
        """
        Convert a row from ERPL to an AirbyteMessage.
        :param row: The row to convert
        :param stream: The stream to which the row belongs
        :return: An AirbyteMessage
        """
        data = {k: v for k, v in zip(columns, row)}
        return AirbyteMessage(type=Type.RECORD, 
                              record=AirbyteRecordMessage(stream=stream.name, data=data, emitted_at=int(time.time())))


    def _create_connection_with_erpl(self, logger: AirbyteLogger, config: json) -> duckdb.DuckDBPyConnection:
        """
        Create a connection with ERPL extension loaded.
        :param logger: The logger object
        :param config: The user input configuration
        :return: A connection with ERPL extension loaded
        """
        logger.info("Createing DuckDB connection with ERPL extension loaded ...")
        
        custom_extension_repository = config["custom_extension_repository"]
        logger.debug("ERPL connection parameters: custom_extension_repository: %s", custom_extension_repository)
        sap_ashost = config["sap_ashost"]
        logger.debug("ERPL connection parameters: sap_ashost: %s", sap_ashost)
        sap_sysnr = config["sap_sysnr"]
        logger.debug("ERPL connection parameters: sap_sysnr: %s", sap_sysnr)
        sap_user = config["sap_user"]
        logger.debug("ERPL connection parameters: sap_user (ends with): %s", sap_user[-1])
        sap_password = config["sap_password"]
        logger.debug("ERPL connection parameters: sap_password (ends with): %s", sap_password[-1])
        sap_client = config["sap_client"]
        logger.debug("ERPL connection parameters: sap_client: %s", sap_client)
        sap_lang = config["sap_lang"]
        logger.debug("ERPL connection parameters: sap_lang: %s", sap_lang)
        
        # Create a connection with ERPL extension loaded
        con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        con.sql(f"SET custom_extension_repository = '{custom_extension_repository}';")
        con.install_extension(config["extension_name"])
        con.load_extension(config["extension_name"])

        # Set ERPL connection parameters 
        con.sql(f"""
            SET sap_ashost = '{sap_ashost}';
            SET sap_sysnr = '{sap_sysnr}';
            SET sap_user = '{sap_user}';
            SET sap_password = '{sap_password}';
            SET sap_client = '{sap_client}';
            SET sap_lang = '{sap_lang}';
        """)

        return con
