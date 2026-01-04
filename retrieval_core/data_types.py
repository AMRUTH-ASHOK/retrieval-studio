"""
Data Type Definitions and Handlers
Supports multiple data types with their own processing logic
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import uuid


@dataclass
class Document:
    """Standard document representation"""
    doc_id: str
    doc_name: str
    text: str
    metadata: Dict[str, Any]
    data_type: str


class DataTypeHandler(ABC):
    """Base class for data type handlers"""
    
    @abstractmethod
    def get_name(self) -> str:
        """Return the name of this data type"""
        pass
    
    @abstractmethod
    def get_display_name(self) -> str:
        """Return user-friendly display name"""
        pass
    
    @abstractmethod
    def get_input_schema(self) -> Dict:
        """Return the input schema for this data type"""
        pass
    
    @abstractmethod
    def load_documents(self, config: Dict[str, Any]) -> List[Document]:
        """Load and convert data into standard Document format"""
        pass
    
    @abstractmethod
    def get_compatible_strategies(self) -> List[str]:
        """Return list of compatible chunking strategies for this data type"""
        pass


class PDFDataType(DataTypeHandler):
    """PDF document handler"""
    
    def get_name(self) -> str:
        return "pdf"
    
    def get_display_name(self) -> str:
        return "PDF Documents"
    
    def get_input_schema(self) -> Dict:
        return {
            "source_type": "upload",
            "file_types": ["pdf"],
            "multiple": True,
            "fields": [
                {"name": "extract_images", "type": "bool", "default": False, "label": "Extract Images"},
                {"name": "ocr_enabled", "type": "bool", "default": False, "label": "Enable OCR"}
            ]
        }
    
    def load_documents(self, config: Dict[str, Any]) -> List[Document]:
        """Load PDF documents"""
        documents = []
        uploaded_files = config.get("uploaded_files", [])
        extract_images = config.get("extract_images", False)
        ocr_enabled = config.get("ocr_enabled", False)
        
        for file in uploaded_files:
            # Placeholder for actual PDF parsing
            # Would use libraries like pymupdf, pdfplumber, pypdf2
            doc = Document(
                doc_id=str(uuid.uuid4()),
                doc_name=file.name if hasattr(file, 'name') else str(file),
                text=self._extract_text_from_pdf(file, ocr_enabled),
                metadata={
                    "extract_images": extract_images,
                    "ocr_enabled": ocr_enabled,
                    "file_size": getattr(file, 'size', 0)
                },
                data_type="pdf"
            )
            documents.append(doc)
        
        return documents
    
    def _extract_text_from_pdf(self, file, ocr_enabled: bool) -> str:
        """Extract text from PDF file"""
        # Placeholder - would implement actual PDF parsing
        return f"[PDF Content from {getattr(file, 'name', 'file')}]"
    
    def get_compatible_strategies(self) -> List[str]:
        return ["baseline", "structured", "parent_child", "semantic"]


class DeltaTableDataType(DataTypeHandler):
    """Delta Table handler"""
    
    def get_name(self) -> str:
        return "delta_table"
    
    def get_display_name(self) -> str:
        return "Delta Table"
    
    def get_input_schema(self) -> Dict:
        return {
            "source_type": "text_input",
            "fields": [
                {"name": "table_name", "type": "text", "required": True, "label": "Table Name (catalog.schema.table)"},
                {"name": "text_column", "type": "text", "required": True, "label": "Text Column Name", "default": "text"},
                {"name": "id_column", "type": "text", "required": False, "label": "ID Column Name (optional)"},
                {"name": "filter_condition", "type": "text", "required": False, "label": "WHERE clause (optional)"}
            ]
        }
    
    def load_documents(self, config: Dict[str, Any]) -> List[Document]:
        """Load documents from Delta Table"""
        from utils.query_builder import escape_identifier, sanitize_string
        
        table_name = config.get("table_name", "")
        text_column = config.get("text_column", "text")
        id_column = config.get("id_column")
        filter_condition = config.get("filter_condition")
        sql_connector = config.get("sql_connector")
        
        if not sql_connector:
            raise ValueError("SQL connector required for Delta Table data type")
        
        # Build query
        table_parts = table_name.split(".")
        if len(table_parts) == 3:
            catalog, schema, table = table_parts
            full_table = f"{escape_identifier(catalog)}.{escape_identifier(schema)}.{escape_identifier(table)}"
        else:
            full_table = escape_identifier(table_name)
        
        id_col = escape_identifier(id_column) if id_column else "uuid()"
        text_col = escape_identifier(text_column)
        
        query = f"SELECT {id_col} as id, {text_col} as text FROM {full_table}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        results = sql_connector.execute(query)
        
        documents = []
        for row in results:
            doc = Document(
                doc_id=str(row.get('id', uuid.uuid4())),
                doc_name=f"row_{row.get('id', uuid.uuid4())}",
                text=str(row.get('text', '')),
                metadata={"source_table": table_name},
                data_type="delta_table"
            )
            documents.append(doc)
        
        return documents
    
    def get_compatible_strategies(self) -> List[str]:
        return ["baseline", "semantic", "sentence"]


class UCVolumeDataType(DataTypeHandler):
    """Unity Catalog Volume handler"""
    
    def get_name(self) -> str:
        return "uc_volume"
    
    def get_display_name(self) -> str:
        return "UC Volume (Files)"
    
    def get_input_schema(self) -> Dict:
        return {
            "source_type": "text_input",
            "fields": [
                {"name": "volume_path", "type": "text", "required": True, "label": "Volume Path"},
                {"name": "file_pattern", "type": "text", "default": "*.txt", "label": "File Pattern"},
                {"name": "recursive", "type": "bool", "default": False, "label": "Search Recursively"}
            ]
        }
    
    def load_documents(self, config: Dict[str, Any]) -> List[Document]:
        """Load documents from UC Volume"""
        volume_path = config.get("volume_path", "")
        file_pattern = config.get("file_pattern", "*.txt")
        recursive = config.get("recursive", False)
        
        # Placeholder for actual implementation
        # Would use dbutils.fs or Volume APIs
        documents = []
        return documents
    
    def get_compatible_strategies(self) -> List[str]:
        return ["baseline", "structured", "parent_child"]


class TextDataType(DataTypeHandler):
    """Plain text input handler"""
    
    def get_name(self) -> str:
        return "text"
    
    def get_display_name(self) -> str:
        return "Plain Text"
    
    def get_input_schema(self) -> Dict:
        return {
            "source_type": "text_area",
            "fields": [
                {"name": "text_content", "type": "textarea", "required": True, "label": "Enter or Paste Text"},
                {"name": "document_name", "type": "text", "default": "text_document", "label": "Document Name"}
            ]
        }
    
    def load_documents(self, config: Dict[str, Any]) -> List[Document]:
        """Load plain text document"""
        text_content = config.get("text_content", "")
        doc_name = config.get("document_name", "text_document")
        
        doc = Document(
            doc_id=str(uuid.uuid4()),
            doc_name=doc_name,
            text=text_content,
            metadata={},
            data_type="text"
        )
        
        return [doc]
    
    def get_compatible_strategies(self) -> List[str]:
        return ["baseline", "semantic", "sentence", "paragraph"]


class CSVDataType(DataTypeHandler):
    """CSV file handler"""
    
    def get_name(self) -> str:
        return "csv"
    
    def get_display_name(self) -> str:
        return "CSV Files"
    
    def get_input_schema(self) -> Dict:
        return {
            "source_type": "upload",
            "file_types": ["csv"],
            "multiple": True,
            "fields": [
                {"name": "text_column", "type": "text", "required": True, "label": "Text Column Name"},
                {"name": "id_column", "type": "text", "required": False, "label": "ID Column (optional)"},
                {"name": "has_header", "type": "bool", "default": True, "label": "Has Header Row"}
            ]
        }
    
    def load_documents(self, config: Dict[str, Any]) -> List[Document]:
        """Load documents from CSV"""
        import csv
        import io
        
        uploaded_files = config.get("uploaded_files", [])
        text_column = config.get("text_column", "")
        id_column = config.get("id_column")
        has_header = config.get("has_header", True)
        
        documents = []
        
        for file in uploaded_files:
            content = file.read() if hasattr(file, 'read') else str(file)
            if isinstance(content, bytes):
                content = content.decode('utf-8')
            
            reader = csv.DictReader(io.StringIO(content)) if has_header else csv.reader(io.StringIO(content))
            
            for idx, row in enumerate(reader):
                if has_header:
                    text = row.get(text_column, "")
                    doc_id = row.get(id_column, str(uuid.uuid4())) if id_column else str(uuid.uuid4())
                else:
                    text = str(row)
                    doc_id = str(uuid.uuid4())
                
                if text:
                    doc = Document(
                        doc_id=str(doc_id),
                        doc_name=f"{getattr(file, 'name', 'csv')}_row_{idx}",
                        text=text,
                        metadata={"source_file": getattr(file, 'name', 'csv'), "row_index": idx},
                        data_type="csv"
                    )
                    documents.append(doc)
        
        return documents
    
    def get_compatible_strategies(self) -> List[str]:
        return ["baseline", "semantic", "sentence"]


class JSONDataType(DataTypeHandler):
    """JSON file handler"""
    
    def get_name(self) -> str:
        return "json"
    
    def get_display_name(self) -> str:
        return "JSON Files"
    
    def get_input_schema(self) -> Dict:
        return {
            "source_type": "upload",
            "file_types": ["json"],
            "multiple": True,
            "fields": [
                {"name": "text_field", "type": "text", "required": True, "label": "Text Field Path (e.g., 'content' or 'data.text')"},
                {"name": "id_field", "type": "text", "required": False, "label": "ID Field Path (optional)"},
                {"name": "is_array", "type": "bool", "default": True, "label": "JSON is an Array"}
            ]
        }
    
    def load_documents(self, config: Dict[str, Any]) -> List[Document]:
        """Load documents from JSON"""
        import json
        
        uploaded_files = config.get("uploaded_files", [])
        text_field = config.get("text_field", "")
        id_field = config.get("id_field")
        is_array = config.get("is_array", True)
        
        documents = []
        
        for file in uploaded_files:
            content = file.read() if hasattr(file, 'read') else str(file)
            if isinstance(content, bytes):
                content = content.decode('utf-8')
            
            data = json.loads(content)
            items = data if is_array else [data]
            
            for idx, item in enumerate(items):
                text = self._get_nested_field(item, text_field)
                doc_id = self._get_nested_field(item, id_field) if id_field else str(uuid.uuid4())
                
                if text:
                    doc = Document(
                        doc_id=str(doc_id),
                        doc_name=f"{getattr(file, 'name', 'json')}_{idx}",
                        text=str(text),
                        metadata={"source_file": getattr(file, 'name', 'json'), "item_index": idx},
                        data_type="json"
                    )
                    documents.append(doc)
        
        return documents
    
    def _get_nested_field(self, obj: Dict, field_path: str) -> Any:
        """Get nested field from object using dot notation"""
        if not field_path:
            return None
        
        parts = field_path.split('.')
        current = obj
        
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        
        return current
    
    def get_compatible_strategies(self) -> List[str]:
        return ["baseline", "semantic", "sentence", "structured"]


# Registry of all available data types
DATA_TYPE_REGISTRY = {
    "pdf": PDFDataType(),
    "delta_table": DeltaTableDataType(),
    "uc_volume": UCVolumeDataType(),
    "text": TextDataType(),
    "csv": CSVDataType(),
    "json": JSONDataType()
}


def get_data_type_handler(data_type: str) -> DataTypeHandler:
    """Get handler for a specific data type"""
    handler = DATA_TYPE_REGISTRY.get(data_type)
    if not handler:
        raise ValueError(f"Unknown data type: {data_type}")
    return handler


def get_all_data_types() -> List[DataTypeHandler]:
    """Get all available data type handlers"""
    return list(DATA_TYPE_REGISTRY.values())
