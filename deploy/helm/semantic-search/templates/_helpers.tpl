{{- define "ss.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 52 | trimSuffix "-" -}}
{{- end }}

{{- define "ss.labels" -}}
app.kubernetes.io/name: {{ .root.Chart.Name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .mode }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version }}
{{- end }}

{{- define "ss.selectorLabels" -}}
app.kubernetes.io/name: {{ .root.Chart.Name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .mode }}
{{- end }}

{{/* Pod spec shared by all modes. Call with (dict "root" $ "mode" "api" "cfg" .Values.api "container" <extra container yaml> "imageTag" <optional tag>). */}}
{{- define "ss.podSpec" -}}
serviceAccountName: {{ include "ss.fullname" .root }}-{{ .mode }}
automountServiceAccountToken: false
{{- if .root.Values.scheduling.enabled }}
# Spread the pods over nodes and zones, so one node or zone failure does not take all of them.
affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          topologyKey: kubernetes.io/hostname
          labelSelector:
            matchLabels:
              {{- include "ss.selectorLabels" (dict "root" .root "mode" .mode) | nindent 14 }}
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels:
        {{- include "ss.selectorLabels" (dict "root" .root "mode" .mode) | nindent 8 }}
{{- end }}
securityContext:
  runAsNonRoot: true
  runAsUser: 10001
  runAsGroup: 10001
  seccompProfile:
    type: RuntimeDefault
containers:
  - name: {{ .mode }}
    image: "{{ .root.Values.image.repository }}:{{ .imageTag | default .root.Values.image.tag | default .root.Chart.AppVersion }}"
    imagePullPolicy: {{ .root.Values.image.pullPolicy }}
    {{- with .cfg.command }}
    command:
      {{- toYaml . | nindent 6 }}
    {{- end }}
    env:
      - name: APP_MODE
        value: {{ .mode | quote }}
    envFrom:
      - configMapRef:
          name: {{ include "ss.fullname" .root }}
      {{- if .root.Values.secretName }}
      # Tokens and keys: APP_API__SERVICE_TOKENS, APP_API__ADMIN_TOKENS and so on, from the secret store.
      - secretRef:
          name: {{ .root.Values.secretName }}
          optional: true
      {{- end }}
    securityContext:
      allowPrivilegeEscalation: false
      readOnlyRootFilesystem: true
      capabilities:
        drop: ["ALL"]
    resources:
      {{- toYaml .cfg.resources | nindent 6 }}
    volumeMounts:
      - name: tmp
        mountPath: /tmp
    {{- if .container }}
    {{- .container | nindent 4 }}
    {{- end }}
volumes:
  - name: tmp
    emptyDir: {}
{{- end }}
