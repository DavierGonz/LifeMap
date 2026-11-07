import jenkins.model.Jenkins

// Conservar asistente y autenticacion; no crear usuarios, credenciales o jobs.
def instance = Jenkins.get()
instance.setNumExecutors(1)
instance.setLabelString('lifemap-docker')
instance.save()
