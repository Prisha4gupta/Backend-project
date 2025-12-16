from etl.extract import DataExtractor
from etl.transform import DataTransformer
from etl.load import DatabaseLoader

extractor = DataExtractor()
transformer = DataTransformer()
loader = DatabaseLoader()

df = extractor.extract_csv("data/enrollments.csv")
df = transformer.transform_enrollments(df)
loader.load_enrollments(df)

print("Enrollments ETL completed")
