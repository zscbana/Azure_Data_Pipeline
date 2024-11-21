-- Customers table
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    Country VARCHAR(50)
);

-- Products table
CREATE TABLE Products (
    StockCode VARCHAR(20) PRIMARY KEY,
    Description VARCHAR(255),
    UnitPrice DECIMAL(10, 2)
);

-- Invoices table
CREATE TABLE Invoices (
    InvoiceNo VARCHAR(20) PRIMARY KEY,
    CustomerID INT,
    InvoiceDate DATETIME,
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);

-- InvoiceItems table
CREATE TABLE InvoiceItems (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    InvoiceNo VARCHAR(20),
    StockCode VARCHAR(20),
    Quantity INT,
    TotalPrice DECIMAL(10, 2),
    FOREIGN KEY (InvoiceNo) REFERENCES Invoices(InvoiceNo),
    FOREIGN KEY (StockCode) REFERENCES Products(StockCode)
);