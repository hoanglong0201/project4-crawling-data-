drop table companies;
drop table address;
drop table ward;
drop table province;
drop table district;
drop table benefits;
drop table jobs;
drop table salaries;
create table companies(
	company_id				int				primary key,
    company_name			varchar(200)	not null,
    country					varchar(50),
    size					varchar(200),
    logo					varchar(200),
    website					varchar(200),
    about_us				varchar(5000)
);
create table ward(
	ward_id					int				primary key,
    ward_name				varchar(100)	
);
create table province(
	province_id					int				primary key,
    province_name				varchar(100)	
);
create table district(
	district_id					int				primary key,
    district_name				varchar(100)	
);

create table address(
	company_id				int,
    company_address_id		int				primary key,
    ward_id					int,
    province_id				int,
    district_id				int,
    street					varchar(200),
    constraint fk_ward	foreign key (ward_id)
    references	ward(ward_id),
    constraint fk_province foreign key (province_id)
    references province(province_id),
    constraint fk_district foreign key (district_id)
    references district(district_id),
    constraint fk_company foreign key (company_id)
    references companies(company_id)
);

create table jobs(
	job_id					int 				primary key,
    job_title				varchar(200),
    company_id				int,
    job_level				varchar(50),
    skill					varchar(200),
    constraint fk_companies foreign key (company_id)
    references	companies(company_id)
);


create table benefits(
	job_id					int 				primary key,
    job_benefits			varchar(2000),
    constraint fk_benefits foreign key (job_id)
    references jobs(job_id)
);


create table salaries(
	job_id					int,
    is_negotiable			tinyint,
    unit					varchar(10),
    min_salary           	int,
    max_salary				int,
    currency				varchar(7),
    min_estimate			int,
    max_estimate			int,
    currency_estimate		varchar(7)
);