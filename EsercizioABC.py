import datetime
import unittest
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, Generic, TypeVar

# Typing and abstract classes
Document = dict[str, Any]
Collection = dict[str, Document]
DatabaseData = dict[str, Collection]

EntityT = TypeVar("EntityT")


class Repository(Generic[EntityT], ABC):
    @abstractmethod
    def create(self, entity: EntityT) -> EntityT: ...

    @abstractmethod
    def update(self, doc_id: str, entity: EntityT) -> EntityT: ...

    @abstractmethod
    def delete(self, doc_id: str) -> bool: ...

    @abstractmethod
    def find_by_id(self, doc_id: str) -> EntityT: ...

    @abstractmethod
    def find_all(self) -> list[EntityT]: ...


class DocumentAlreadyExists(Exception): ...


class DocumentNotFound(Exception):
    ...


class Database:
    def __init__(self) -> None:
        self._data: DatabaseData | None = None

    def start(self) -> None:
        self._data = DatabaseData()

    def stop(self) -> None:
        self._data = None

    def insert(self, collection: str, doc_id: str, data: dict[str, Any]) -> bool:
        if self._data is not None:
            coll = self._data.get(collection, Collection())
            coll[doc_id] = data
            self._data[collection] = coll
            return True
        raise ConnectionError()

    def remove(self, collection: str, doc_id: str) -> bool:
        if self._data is not None:
            if doc_id in self._data.get(collection):
                coll = self._data.get(collection, Collection())
                del coll[doc_id]
                self._data[collection] = coll
                return True
            return False
        raise ConnectionError()

    def fetch_collection(self, collection: str) -> Collection:
        if self._data is not None:
            return self._data.get(collection, Collection())
        raise ConnectionError()

    @property
    def data(self):
        return self._data


# Your code here
@dataclass
class Product:
    product_id: str
    name: str
    description: str
    price: float
    created: datetime.datetime
    updated: datetime.datetime


class ProductRepository(Repository[Product]):
    def __init__(self, database: Database) -> None:
        self.client = database

    def create(self, entity: Product) -> Product:
        exists = self.client.fetch_collection("product").get(entity.product_id)
        if not exists:
            self.client.insert(
                collection="product", doc_id=entity.product_id, data=asdict(entity)
            )
            return entity
        else:
            raise DocumentAlreadyExists("Oh no! This product already exists!")

    def update(self, doc_id: str, entity: Product) -> Product:
        exists = self.client.fetch_collection("product").get(doc_id)
        if exists:
            self.client.insert(
                collection="product", doc_id=entity.product_id, data=asdict(entity)
            )
            return entity
        else:
            raise DocumentNotFound("Oh no! Product not found!")

    def find_all(self) -> list[Product]:
        products = self.client.fetch_collection("product")
        if len(products) == 0:
            raise DocumentNotFound("No documents are present")
        return [Product(**single_products) for single_products in products.values()]

    def delete(self, doc_id: str) -> bool:
        exists = self.client.fetch_collection("product").get(doc_id)
        if exists:
            return self.client.remove(collection="product", doc_id=doc_id)
        return False

    def find_by_id(self, doc_id: str) -> Product:
        product_dict = self.client.fetch_collection("product").get(doc_id)
        if product_dict:
            return Product(**product_dict)
        raise DocumentNotFound(f"Document with id: {doc_id} not present ")


class TestProductRepository(unittest.TestCase):
    @staticmethod
    def retrieve_product_repo():
        db = Database()
        db.start()
        return ProductRepository(db)

    def test_find_all_returns_all_elements(self):
        product_repo = TestProductRepository.retrieve_product_repo()
        product_repo.create(Product("1", "Test", "Test", 10.0, datetime.datetime.now(), datetime.datetime.now()))

        product_list = product_repo.find_all()
        self.assertEquals(1, len(product_list))
        self.assertEquals("1", product_list[0].product_id)

    def test_find_all_raise_exception_if_no_elements_found(self):
        product_repo = TestProductRepository.retrieve_product_repo()

        self.assertRaises(DocumentNotFound, product_repo.find_all)

    def test_delete_correctly_remove_element(self):
        product_repo = TestProductRepository.retrieve_product_repo()
        product_repo.create(Product("1", "Test", "Test", 10.0, datetime.datetime.now(), datetime.datetime.now()))
        product_repo.create(Product("2", "Test", "Test", 10.0, datetime.datetime.now(), datetime.datetime.now()))

        product_repo.delete("2")

        self.assertEqual(1, len(product_repo.find_all()))

    def test_delete_correctly_return_false_if_no_document_is_found(self):
        product_repo = TestProductRepository.retrieve_product_repo()

        self.assertFalse(product_repo.delete("2"))

    def test_find_by_id_return_single_element(self):
        product_repo = TestProductRepository.retrieve_product_repo()
        product_repo.create(Product("1", "Test", "Test", 10.0, datetime.datetime.now(), datetime.datetime.now()))
        product_repo.create(Product("2", "Test", "Test", 10.0, datetime.datetime.now(), datetime.datetime.now()))

        product = product_repo.find_by_id("2")

        self.assertEqual("2", product.product_id)
        self.assertEquals("Test", product.name)
        self.assertEquals("Test", product.description)

    def test_find_by_id_raise_exception_if_no_elements_found(self):
        product_repo = TestProductRepository.retrieve_product_repo()

        self.assertRaises(DocumentNotFound, product_repo.find_by_id, "2")


if __name__ == "__main__":
    unittest.main()
