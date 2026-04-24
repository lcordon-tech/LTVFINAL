# 📊 LTV System - Customer Lifetime Value Analytics

Sistema CLI multi-país para cálculo de LTV en e-commerce. Procesa órdenes MySQL, calcula cohortes (trimestral/mensual/semanal), y analiza por categoría/marca/producto.

## 🚀 Opciones principales

| Opción | Función |
|--------|---------|
| **1** | Pipeline completo (DR + MD) |
| **2** | Solo Data Repository |
| **3** | Modelo (completo/general/categoría/subcategoría/marca/producto/especial/solo heavy) |
| **4** | Buscador interactivo |
| **5** | Configuraciones (modos, carpetas, gestión de cohortes) |
| **b** | Cambiar país (GT/CR) |
| **q** | Salir |

## ⚙️ Configuraciones

- **Modo agrupación**: entry_based / behavioral
- **Modo marca**: plano / jerárquico / dual  
- **Granularidad**: trimestral / mensual / semanal / semestral / anual
- **Gestión de cohortes**: CRUD completo

## 🔐 Autenticación

- Usuarios con bcrypt
- Credenciales DB globales
- SSH por país (GT/CR)
- Almacenamiento cifrado (Fernet)

## 📁 Archivos necesarios

- `SOIS.xlsx` (hoja: GT/CR)
- `SUPUESTOS.xlsx` (hojas: 1PGT, 3PGT, ...)
- `catalogLTV.xlsx` (hoja: GT/CR)
- `CAC.xlsx`, `TIPO_DE_CAMBIO.xlsx`

## 🌍 Países soportados

| Código | Moneda | Inicio cohortes |
|--------|--------|-----------------|
| GT | GTQ | 2021 |
| CR | CRC | 2022 |

## 🛠️ Requisitos

```bash
pip install pandas sqlalchemy pymysql cryptography bcrypt openpyxl
