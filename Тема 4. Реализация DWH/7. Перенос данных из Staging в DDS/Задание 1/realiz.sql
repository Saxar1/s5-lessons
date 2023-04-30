-- stg.srv_wf_settings definition

-- Drop table

-- DROP TABLE stg.srv_wf_settings;

CREATE TABLE dds.srv_wf_settings (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);