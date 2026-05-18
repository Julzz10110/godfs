{{- define "godfs.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "godfs.labels" -}}
app.kubernetes.io/name: {{ include "godfs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "godfs.masterRaftPeers" -}}
{{- $ns := .Values.namespace -}}
{{- $rel := .Release.Name -}}
{{- $raftPort := .Values.master.raftPort -}}
{{- $grpcPort := .Values.master.grpcPort -}}
{{- range $i, $e := until (.Values.raft.replicas | int) -}}
{{- if $i }},{{ end -}}
{{ $rel }}-master-{{ $i }}@{{ $rel }}-master-{{ $i }}.{{ $rel }}-master-hs.{{ $ns }}.svc.cluster.local:{{ $raftPort }}@{{ $rel }}-master-{{ $i }}.{{ $rel }}-master-hs.{{ $ns }}.svc.cluster.local:{{ $grpcPort }}
{{- end -}}
{{- end -}}

