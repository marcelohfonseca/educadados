CREATE TABLE IF NOT EXISTS `EDUCADADOS`.`DIM_CURSO_IES` (
    `NR_ANO` INT NOT NULL,
    `CD_CURSO` INT NOT NULL,
    `NM_CURSO` VARCHAR(255) NOT NULL,
    `CD_AREA` INT NULL,
    `NM_AREA` VARCHAR(255) NULL,
    `CD_AREA_ESPECIFICA` INT NULL,
    `NM_AREA_ESPECIFICA` VARCHAR(255) NULL,
    `CD_AREA_DETALHE` INT NULL,
    `NM_AREA_DETALHE` VARCHAR(255) NULL,
    `CD_GRAU_ACADEMICO` INT NULL,
    `NM_GRAU_ACADEMICO` VARCHAR(255) NULL,
    `CD_MODALIDADE_ENSINO` INT NULL,
    `NM_MODALIDADE_ENSINO` VARCHAR(255) NULL,
    `CD_NIVEL_ACADEMICO` INT NULL,
    `NM_NIVEL_ACADEMICO` VARCHAR(255) NULL,
    `CD_IES` INT NULL,
    `CD_REDE` INT NULL,
    PRIMARY KEY (`NR_ANO`, `CD_CURSO`),
);