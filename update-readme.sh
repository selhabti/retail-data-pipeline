#!/bin/bash

# update-readme.sh - Version simplifiée et robuste
# Met à jour automatiquement l'arborescence dans README.md

set -euo pipefail

# Configuration
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly README_FILE="${SCRIPT_DIR}/README.md"
readonly BACKUP_FILE="${README_FILE}.backup"

# Couleurs
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m'

# Logging
log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

# Vérifier tree
check_tree() {
    if ! command -v tree >/dev/null 2>&1; then
        log_error "La commande 'tree' n'est pas installée."
        log_info "Installation: sudo apt install tree"
        exit 1
    fi
}

# Générer l'arborescence
generate_tree() {
    log_info "Génération de l'arborescence..."
    
    # Exclusions
    local excludes='node_modules|.git|__pycache__|.venv|venv|env|.env'
    excludes+='|dist|build|target|.DS_Store|*.log|.cache|.pytest_cache'
    excludes+='|.coverage|.terraform|README.md.backup|*.tmp'
    
    # Générer avec emojis, sans couleurs ANSI
    tree -I "$excludes" --dirsfirst -a --charset ascii --noreport | sed -E '
        s/^(\|-- |\`-- )airflow/\1🎯 airflow/
        s/^(\|-- |\`-- )cloud_functions/\1☁️ cloud_functions/
        s/^(\|-- |\`-- )\.github/\1🚀 .github/
        s/^(\|-- |\`-- )scripts/\1🛠️ scripts/
        s/^(\|-- |\`-- )terraform/\1🏗️ terraform/
        s/^(\|-- |\`-- )tests/\1🧪 tests/
    ' | sed -r "s/\x1B\[[0-9;]*[mK]//g"  # Supprime séquences ANSI
}

# Créer sauvegarde
create_backup() {
    if [[ -f "$README_FILE" ]]; then
        cp "$README_FILE" "$BACKUP_FILE"
        log_info "Sauvegarde créée"
    fi
}

# Mettre à jour README
update_readme() {
    if [[ ! -f "$README_FILE" ]]; then
        log_error "README.md non trouvé"
        exit 1
    fi
    
    local tree_content
    tree_content=$(generate_tree)
    local timestamp
    timestamp=$(date '+%Y-%m-%d à %H:%M:%S')
    
    # Vérifier les marqueurs
    if ! grep -q "<!-- TREE_START -->" "$README_FILE"; then
        log_warning "Marqueurs non trouvés, ajout en fin de fichier"
        cat >> "$README_FILE" << EOF

<!-- TREE_START -->
## 📁 Structure du projet

\`\`\`
$tree_content
\`\`\`

*Arborescence générée automatiquement le $timestamp*
<!-- TREE_END -->
EOF
        log_success "Section arborescence ajoutée"
        return
    fi
    
    # Créer le nouveau contenu
    local new_section
    new_section=$(cat << EOF
<!-- TREE_START -->
## 📁 Structure du projet

\`\`\`
$tree_content
\`\`\`

*Arborescence générée automatiquement le $timestamp*
<!-- TREE_END -->
EOF
)
    
    # Remplacer la section
    local temp_file
    temp_file=$(mktemp)
    
    # Utiliser Python pour un remplacement sûr
    python3 << PYTHON_SCRIPT > "$temp_file"
import re

with open('$README_FILE', 'r') as f:
    content = f.read()

# Remplacer la section entre les marqueurs
pattern = r'<!-- TREE_START -->.*?<!-- TREE_END -->'
replacement = '''$new_section'''

new_content = re.sub(pattern, replacement, content, flags=re.DOTALL)
print(new_content, end='')
PYTHON_SCRIPT
    
    mv "$temp_file" "$README_FILE"
    log_success "Section arborescence mise à jour"
}

# Valider
validate() {
    if ! grep -q "<!-- TREE_START -->" "$README_FILE" || ! grep -q "<!-- TREE_END -->" "$README_FILE"; then
        log_error "Marqueurs manquants après mise à jour"
        return 1
    fi
    
    if [[ ! -s "$README_FILE" ]]; then
        log_error "README vide"
        return 1
    fi
    
    log_success "Validation réussie"
}

# Nettoyage
cleanup() {
    [[ -f "$BACKUP_FILE" ]] && rm -f "$BACKUP_FILE"
}

# Gestion erreur
error_handler() {
    log_error "Erreur détectée, restauration..."
    if [[ -f "$BACKUP_FILE" ]]; then
        cp "$BACKUP_FILE" "$README_FILE"
        log_info "README restauré"
    fi
    cleanup
    exit 1
}

# Usage
show_usage() {
    echo "Usage: $0 [--dry-run] [--help] [--commit-message <message>]"
    echo "  --dry-run            Prévisualisation sans modification"
    echo "  --help               Afficher cette aide"
    echo "  --commit-message     Message de commit personnalisé"
}

# Main
main() {
    local dry_run=false
    local commit_message="Mise à jour automatique de l'arborescence dans README.md"
    
    # Arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --dry-run)
                dry_run=true
                shift
                ;;
            --help)
                show_usage
                exit 0
                ;;
            --commit-message)
                if [[ -n "$2" ]]; then
                    commit_message="$2"
                    shift 2
                else
                    log_error "Option --commit-message requiert un argument"
                    show_usage
                    exit 1
                fi
                ;;
            *)
                log_error "Option inconnue: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    trap error_handler ERR
    
    log_info "Démarrage mise à jour arborescence"
    
    check_tree
    create_backup
    
    if [[ "$dry_run" == true ]]; then
        log_info "=== MODE DRY-RUN ==="
        generate_tree
        log_info "Aucune modification (dry-run)"
    else
        update_readme
        validate
        log_success "Mise à jour terminée !"
    fi
    
    trap - ERR
    cleanup
}

# Exécution
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
