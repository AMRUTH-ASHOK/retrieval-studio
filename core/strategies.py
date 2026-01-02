from abc import ABC, abstractmethod
from typing import List, Dict, Any
from dataclasses import dataclass
import uuid

@dataclass
class Chunk:
    chunk_id: str
    doc_id: str
    doc_name: str
    chunk_text: str
    chunk_index: int
    metadata: Dict[str, Any]
    parent_chunk_id: str = None

class ChunkingStrategy(ABC):
    """Base class for chunking strategies"""
    
    @abstractmethod
    def chunk(self, documents: List[Dict]) -> List[Chunk]:
        """
        Chunk documents according to strategy
        
        Args:
            documents: List of document dicts with 'doc_id', 'doc_name', 'text', etc.
        
        Returns:
            List of Chunk objects
        """
        pass
    
    @classmethod
    @abstractmethod
    def get_config_schema(cls) -> Dict:
        """Return configuration schema for this strategy"""
        pass

class BaselineStrategy(ChunkingStrategy):
    """Fixed-size chunking strategy"""
    
    def __init__(self, chunk_size: int = 512, overlap: int = 50):
        self.chunk_size = chunk_size
        self.overlap = overlap
    
    def chunk(self, documents: List[Dict]) -> List[Chunk]:
        chunks = []
        
        for doc in documents:
            text = doc.get("text", "")
            doc_id = doc.get("doc_id", "")
            doc_name = doc.get("doc_name", "")
            
            if not text:
                continue
            
            # Simple fixed-size chunking
            start = 0
            chunk_index = 0
            
            while start < len(text):
                end = min(start + self.chunk_size, len(text))
                chunk_text = text[start:end]
                
                chunk = Chunk(
                    chunk_id=str(uuid.uuid4()),
                    doc_id=doc_id,
                    doc_name=doc_name,
                    chunk_text=chunk_text,
                    chunk_index=chunk_index,
                    metadata={
                        "strategy": "baseline",
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "start_char": start,
                        "end_char": end,
                    }
                )
                chunks.append(chunk)
                
                start += (self.chunk_size - self.overlap)
                chunk_index += 1
        
        return chunks
    
    @classmethod
    def get_config_schema(cls) -> Dict:
        return {
            "chunk_size": {"type": "int", "default": 512, "min": 100, "max": 2048},
            "overlap": {"type": "int", "default": 50, "min": 0, "max": 200},
        }

class StructuredStrategy(ChunkingStrategy):
    """Section-aware chunking strategy"""
    
    def __init__(self, preserve_hierarchy: bool = True, max_chunk_size: int = 1024):
        self.preserve_hierarchy = preserve_hierarchy
        self.max_chunk_size = max_chunk_size
    
    def chunk(self, documents: List[Dict]) -> List[Chunk]:
        chunks = []
        
        for doc in documents:
            # Parse document structure (headings, sections, etc.)
            # This is simplified - real implementation would parse PDF structure
            text = doc.get("text", "")
            doc_id = doc.get("doc_id", "")
            doc_name = doc.get("doc_name", "")
            
            if not text:
                continue
            
            # Split by headings (simplified)
            sections = self._extract_sections(text)
            
            chunk_index = 0
            for section in sections:
                section_text = section.get("text", "")
                section_path = section.get("path", "")
                
                # Chunk section if too large
                if len(section_text) > self.max_chunk_size:
                    # Further chunk this section
                    sub_chunks = self._chunk_text(section_text, self.max_chunk_size)
                    for sub_chunk_text in sub_chunks:
                        chunk = Chunk(
                            chunk_id=str(uuid.uuid4()),
                            doc_id=doc_id,
                            doc_name=doc_name,
                            chunk_text=sub_chunk_text,
                            chunk_index=chunk_index,
                            metadata={
                                "strategy": "structured",
                                "section_path": section_path,
                                "preserve_hierarchy": self.preserve_hierarchy,
                            }
                        )
                        chunks.append(chunk)
                        chunk_index += 1
                else:
                    chunk = Chunk(
                        chunk_id=str(uuid.uuid4()),
                        doc_id=doc_id,
                        doc_name=doc_name,
                        chunk_text=section_text,
                        chunk_index=chunk_index,
                        metadata={
                            "strategy": "structured",
                            "section_path": section_path,
                            "preserve_hierarchy": self.preserve_hierarchy,
                        }
                    )
                    chunks.append(chunk)
                    chunk_index += 1
        
        return chunks
    
    def _extract_sections(self, text: str) -> List[Dict]:
        """Extract sections from text (simplified - would use PDF parsing in real implementation)"""
        # Placeholder - would use libraries like pymupdf, pdfplumber, etc.
        # For now, split by double newlines as a simple heuristic
        if not text or not text.strip():
            return []
        
        sections = []
        parts = text.split("\n\n")
        for i, part in enumerate(parts):
            if part.strip():
                sections.append({
                    "text": part.strip(),
                    "path": f"section_{i}"
                })
        return sections if sections else [{"text": text.strip(), "path": "root"}]
    
    def _chunk_text(self, text: str, max_size: int) -> List[str]:
        """Chunk text into smaller pieces"""
        chunks = []
        start = 0
        while start < len(text):
            end = min(start + max_size, len(text))
            chunks.append(text[start:end])
            start = end
        return chunks
    
    @classmethod
    def get_config_schema(cls) -> Dict:
        return {
            "preserve_hierarchy": {"type": "bool", "default": True},
            "max_chunk_size": {"type": "int", "default": 1024, "min": 256, "max": 2048},
        }

class ParentChildStrategy(ChunkingStrategy):
    """Parent-child chunking strategy"""
    
    def __init__(self, parent_size: int = 2048, child_size: int = 256):
        self.parent_size = parent_size
        self.child_size = child_size
    
    def chunk(self, documents: List[Dict]) -> List[Chunk]:
        chunks = []
        
        for doc in documents:
            text = doc.get("text", "")
            doc_id = doc.get("doc_id", "")
            doc_name = doc.get("doc_name", "")
            
            if not text:
                continue
            
            # Create parent chunks
            parent_chunks = self._create_parent_chunks(text, doc_id, doc_name)
            
            # Create child chunks for each parent
            for parent in parent_chunks:
                children = self._create_child_chunks(
                    parent.chunk_text,
                    parent.chunk_id,
                    doc_id,
                    doc_name
                )
                chunks.append(parent)
                chunks.extend(children)
        
        return chunks
    
    def _create_parent_chunks(self, text: str, doc_id: str, doc_name: str) -> List[Chunk]:
        """Create parent chunks"""
        parents = []
        start = 0
        parent_index = 0
        
        while start < len(text):
            end = min(start + self.parent_size, len(text))
            parent_text = text[start:end]
            
            parent = Chunk(
                chunk_id=str(uuid.uuid4()),
                doc_id=doc_id,
                doc_name=doc_name,
                chunk_text=parent_text,
                chunk_index=parent_index,
                metadata={
                    "strategy": "parent_child",
                    "chunk_type": "parent",
                    "parent_size": self.parent_size,
                }
            )
            parents.append(parent)
            
            start = end
            parent_index += 1
        
        return parents
    
    def _create_child_chunks(self, parent_text: str, parent_id: str, 
                            doc_id: str, doc_name: str) -> List[Chunk]:
        """Create child chunks from parent"""
        children = []
        start = 0
        child_index = 0
        
        while start < len(parent_text):
            end = min(start + self.child_size, len(parent_text))
            child_text = parent_text[start:end]
            
            child = Chunk(
                chunk_id=str(uuid.uuid4()),
                doc_id=doc_id,
                doc_name=doc_name,
                chunk_text=child_text,
                chunk_index=child_index,
                metadata={
                    "strategy": "parent_child",
                    "chunk_type": "child",
                    "child_size": self.child_size,
                },
                parent_chunk_id=parent_id
            )
            children.append(child)
            
            start = end
            child_index += 1
        
        return children
    
    @classmethod
    def get_config_schema(cls) -> Dict:
        return {
            "parent_size": {"type": "int", "default": 2048, "min": 512, "max": 4096},
            "child_size": {"type": "int", "default": 256, "min": 128, "max": 1024},
        }

class SemanticStrategy(ChunkingStrategy):
    """Semantic chunking based on meaning similarity"""
    
    def __init__(self, similarity_threshold: float = 0.7, min_chunk_size: int = 100, max_chunk_size: int = 1024):
        self.similarity_threshold = similarity_threshold
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
    
    def chunk(self, documents: List[Dict]) -> List[Chunk]:
        chunks = []
        
        for doc in documents:
            text = doc.get("text", "")
            doc_id = doc.get("doc_id", "")
            doc_name = doc.get("doc_name", "")
            
            if not text:
                continue
            
            # Split into sentences first
            sentences = self._split_sentences(text)
            
            # Group semantically similar sentences
            semantic_chunks = self._group_by_semantics(sentences)
            
            chunk_index = 0
            for chunk_text in semantic_chunks:
                chunk = Chunk(
                    chunk_id=str(uuid.uuid4()),
                    doc_id=doc_id,
                    doc_name=doc_name,
                    chunk_text=chunk_text,
                    chunk_index=chunk_index,
                    metadata={
                        "strategy": "semantic",
                        "similarity_threshold": self.similarity_threshold,
                    }
                )
                chunks.append(chunk)
                chunk_index += 1
        
        return chunks
    
    def _split_sentences(self, text: str) -> List[str]:
        """Split text into sentences (simplified)"""
        import re
        sentences = re.split(r'[.!?]\s+', text)
        return [s.strip() for s in sentences if s.strip()]
    
    def _group_by_semantics(self, sentences: List[str]) -> List[str]:
        """Group sentences by semantic similarity (simplified)"""
        # Placeholder - would use embeddings and cosine similarity
        # For now, group by size
        chunks = []
        current_chunk = []
        current_size = 0
        
        for sentence in sentences:
            sentence_len = len(sentence)
            
            if current_size + sentence_len > self.max_chunk_size and current_chunk:
                chunks.append(" ".join(current_chunk))
                current_chunk = [sentence]
                current_size = sentence_len
            else:
                current_chunk.append(sentence)
                current_size += sentence_len
        
        if current_chunk:
            chunks.append(" ".join(current_chunk))
        
        return chunks
    
    @classmethod
    def get_config_schema(cls) -> Dict:
        return {
            "similarity_threshold": {"type": "float", "default": 0.7, "min": 0.0, "max": 1.0},
            "min_chunk_size": {"type": "int", "default": 100, "min": 50, "max": 500},
            "max_chunk_size": {"type": "int", "default": 1024, "min": 256, "max": 2048},
        }

class SentenceStrategy(ChunkingStrategy):
    """Sentence-based chunking"""
    
    def __init__(self, sentences_per_chunk: int = 5, overlap_sentences: int = 1):
        self.sentences_per_chunk = sentences_per_chunk
        self.overlap_sentences = overlap_sentences
    
    def chunk(self, documents: List[Dict]) -> List[Chunk]:
        chunks = []
        
        for doc in documents:
            text = doc.get("text", "")
            doc_id = doc.get("doc_id", "")
            doc_name = doc.get("doc_name", "")
            
            if not text:
                continue
            
            # Split into sentences
            sentences = self._split_sentences(text)
            
            chunk_index = 0
            i = 0
            
            while i < len(sentences):
                end = min(i + self.sentences_per_chunk, len(sentences))
                chunk_sentences = sentences[i:end]
                chunk_text = " ".join(chunk_sentences)
                
                chunk = Chunk(
                    chunk_id=str(uuid.uuid4()),
                    doc_id=doc_id,
                    doc_name=doc_name,
                    chunk_text=chunk_text,
                    chunk_index=chunk_index,
                    metadata={
                        "strategy": "sentence",
                        "sentences_per_chunk": self.sentences_per_chunk,
                        "overlap_sentences": self.overlap_sentences,
                        "num_sentences": len(chunk_sentences),
                    }
                )
                chunks.append(chunk)
                
                i += (self.sentences_per_chunk - self.overlap_sentences)
                chunk_index += 1
        
        return chunks
    
    def _split_sentences(self, text: str) -> List[str]:
        """Split text into sentences"""
        import re
        sentences = re.split(r'[.!?]\s+', text)
        return [s.strip() for s in sentences if s.strip()]
    
    @classmethod
    def get_config_schema(cls) -> Dict:
        return {
            "sentences_per_chunk": {"type": "int", "default": 5, "min": 1, "max": 20},
            "overlap_sentences": {"type": "int", "default": 1, "min": 0, "max": 5},
        }

class ParagraphStrategy(ChunkingStrategy):
    """Paragraph-based chunking"""
    
    def __init__(self, paragraphs_per_chunk: int = 2, max_chunk_size: int = 1500):
        self.paragraphs_per_chunk = paragraphs_per_chunk
        self.max_chunk_size = max_chunk_size
    
    def chunk(self, documents: List[Dict]) -> List[Chunk]:
        chunks = []
        
        for doc in documents:
            text = doc.get("text", "")
            doc_id = doc.get("doc_id", "")
            doc_name = doc.get("doc_name", "")
            
            if not text:
                continue
            
            # Split into paragraphs
            paragraphs = self._split_paragraphs(text)
            
            chunk_index = 0
            i = 0
            
            while i < len(paragraphs):
                current_chunk = []
                current_size = 0
                
                while i < len(paragraphs) and len(current_chunk) < self.paragraphs_per_chunk:
                    para = paragraphs[i]
                    if current_size + len(para) > self.max_chunk_size and current_chunk:
                        break
                    current_chunk.append(para)
                    current_size += len(para)
                    i += 1
                
                if current_chunk:
                    chunk_text = "\n\n".join(current_chunk)
                    
                    chunk = Chunk(
                        chunk_id=str(uuid.uuid4()),
                        doc_id=doc_id,
                        doc_name=doc_name,
                        chunk_text=chunk_text,
                        chunk_index=chunk_index,
                        metadata={
                            "strategy": "paragraph",
                            "paragraphs_per_chunk": self.paragraphs_per_chunk,
                            "num_paragraphs": len(current_chunk),
                        }
                    )
                    chunks.append(chunk)
                    chunk_index += 1
        
        return chunks
    
    def _split_paragraphs(self, text: str) -> List[str]:
        """Split text into paragraphs"""
        paragraphs = text.split("\n\n")
        return [p.strip() for p in paragraphs if p.strip()]
    
    @classmethod
    def get_config_schema(cls) -> Dict:
        return {
            "paragraphs_per_chunk": {"type": "int", "default": 2, "min": 1, "max": 10},
            "max_chunk_size": {"type": "int", "default": 1500, "min": 500, "max": 3000},
        }

# Strategy Registry
STRATEGY_REGISTRY = {
    "baseline": BaselineStrategy,
    "structured": StructuredStrategy,
    "parent_child": ParentChildStrategy,
    "semantic": SemanticStrategy,
    "sentence": SentenceStrategy,
    "paragraph": ParagraphStrategy,
}

# Strategy Descriptions
STRATEGY_DESCRIPTIONS = {
    "baseline": "Fixed-size chunking with configurable overlap. Simple and effective for general use cases.",
    "structured": "Section-aware chunking that preserves document hierarchy and structure.",
    "parent_child": "Creates parent chunks with smaller child chunks for hierarchical retrieval.",
    "semantic": "Groups content based on semantic similarity using embeddings.",
    "sentence": "Chunks based on sentence boundaries with configurable overlap.",
    "paragraph": "Chunks based on paragraph boundaries, preserving natural text divisions.",
}

def get_strategy(strategy_name: str, **kwargs) -> ChunkingStrategy:
    """Get a strategy instance by name"""
    strategy_class = STRATEGY_REGISTRY.get(strategy_name)
    if not strategy_class:
        raise ValueError(f"Unknown strategy: {strategy_name}")
    return strategy_class(**kwargs)

def get_all_strategies() -> List[Dict]:
    """Get all available strategies with their metadata"""
    strategies = []
    for name, strategy_class in STRATEGY_REGISTRY.items():
        strategies.append({
            "name": name,
            "display_name": name.replace("_", " ").title(),
            "description": STRATEGY_DESCRIPTIONS.get(name, ""),
            "parameters": strategy_class.get_config_schema()
        })
    return strategies

def get_strategy_class(strategy_name: str) -> type:
    """Get a strategy class by name"""
    return STRATEGY_REGISTRY.get(strategy_name)

