{{- define "godfs.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "godfs.labels" -}}
app.kubernetes.io/name: {{ include "godfs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

