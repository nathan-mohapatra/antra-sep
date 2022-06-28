DROP TABLE IF EXISTS [dbo].[watermarktable];

CREATE TABLE watermarktable (
	TableName VARCHAR(255),
	WatermarkValue DATETIME
);

INSERT INTO watermarktable
VALUES
('orders_table', '2013-01-01T11:00:00'),
('order_lines_table', '2013-01-01T11:00:00');