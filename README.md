# mongoconn

![GitHub contributors](https://img.shields.io/github/contributors/sivaosorg/gocell)
![GitHub followers](https://img.shields.io/github/followers/sivaosorg)
![GitHub User's stars](https://img.shields.io/github/stars/pnguyen215)

A Golang MongoDB connector library with a comprehensive set of features for interacting with MongoDB databases, including CRUD operations, aggregation, and file handling.

## Table of Contents

- [mongoconn](#mongoconn)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Prerequisites](#prerequisites)
  - [Key Features](#key-features)
  - [Installation](#installation)
  - [Modules](#modules)
    - [Running Tests](#running-tests)
    - [Tidying up Modules](#tidying-up-modules)
    - [Upgrading Dependencies](#upgrading-dependencies)
    - [Cleaning Dependency Cache](#cleaning-dependency-cache)

## Introduction

Welcome to the MongoDB Connector for Go repository! This library provides a powerful set of tools for seamless interaction with MongoDB databases in your Go applications. It supports a wide range of functionalities, including document creation, retrieval, updating, deletion, aggregation, and file handling.

## Prerequisites

Golang version v1.20

## Key Features

- CRUD Operations: Create, read, update, and delete documents in MongoDB with ease.
- Aggregation Framework: Leverage MongoDB's aggregation framework to perform complex data manipulations.
- Transaction Support: Conduct transactions securely with MongoDB's transaction functionality.
- File Handling: Upload and download files to and from MongoDB GridFS.
- Database Backup and Restore: Backup and restore your MongoDB databases effortlessly.

## Installation

- Latest version

```bash
go get -u github.com/sivaosorg/mongoconn@latest
```

- Use a specific version (tag)

```bash
go get github.com/sivaosorg/mongoconn@v0.0.1
```

## Modules

Explain how users can interact with the various modules.

### Running Tests

To run tests for all modules, use the following command:

```bash
make test
```

### Tidying up Modules

To tidy up the project's Go modules, use the following command:

```bash
make tidy
```

### Upgrading Dependencies

To upgrade project dependencies, use the following command:

```bash
make deps-upgrade
```

### Cleaning Dependency Cache

To clean the Go module cache, use the following command:

```bash
make deps-clean-cache
```
