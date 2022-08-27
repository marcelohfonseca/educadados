CREATE TABLE IF NOT EXISTS `EDUCADADOS`.`DIM_MUNICIPIO` (
    `CD_MUNICIPIO` INT PRIMARY KEY,
    `NM_MUNICIPIO` VARCHAR(255) NOT NULL,
    `CD_UF` INT NULL,
    `SG_UF` VARCHAR(2) NULL,
    `NM_UF` VARCHAR(255) NULL,
    `CD_MESOREGIAO` INT NULL,
    `NM_MESOREGIAO` VARCHAR(255) NULL,
    `CD_MICROREGIAO` INT NULL,
    `NM_MICROREGIAO` VARCHAR(255) NULL,
    `CD_REGIAO` INT NULL,
    `NM_REGIAO` VARCHAR(255) NULL,
    `FL_CAPITAL` CHAR(1) NULL,
);